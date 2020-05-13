<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Throwable;
use ErrorException;
use PDO;
use PDOStatement;
use PDOException;
use Psr\Log\LoggerInterface;
use Keboola\DbExtractor\TableResultFormat\Metadata\GetTables\DefaultGetTablesSerializer;
use Keboola\DbExtractor\TableResultFormat\Metadata\GetTables\GetTablesSerializer;
use Keboola\Csv\CsvWriter;
use Keboola\Csv\Exception as CsvException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\DeadConnectionException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\TableResultFormat\Metadata\Manifest\DefaultManifestSerializer;
use Keboola\DbExtractor\TableResultFormat\Metadata\Manifest\ManifestSerializer;
use Keboola\DbExtractorSSHTunnel\SSHTunnel;
use Keboola\DbExtractorSSHTunnel\Exception\UserException as SSHTunnelUserException;
use Nette\Utils;

abstract class BaseExtractor
{
    public const CONNECT_MAX_RETRIES = 5;

    /** @var PDO|mixed */
    protected $db;

    protected array $state;

    protected LoggerInterface $logger;

    protected string $dataDir;

    private array $dbParameters;

    public function __construct(array $parameters, array $state, LoggerInterface $logger)
    {
        $this->logger = $logger;
        $this->dataDir = $parameters['data_dir'];
        $this->state = $state;
        if (isset($parameters['db']['ssh']['enabled']) && $parameters['db']['ssh']['enabled']) {
            try {
                $sshTunnel = new SSHTunnel($logger);
                $parameters['db'] = $sshTunnel->createSshTunnel($parameters['db']);
            } catch (SSHTunnelUserException $e) {
                throw new UserException($e->getMessage(), 0, $e);
            }
        }
        $this->dbParameters = $parameters['db'];

        $proxy = new DbRetryProxy($this->logger, self::CONNECT_MAX_RETRIES, [PDOException::class]);

        try {
            $proxy->call(function (): void {
                $this->db = $this->createConnection($this->dbParameters);
            });
        } catch (PDOException $e) {
            throw new UserException('Error connecting to DB: ' . $e->getMessage(), 0, $e);
        } catch (Throwable $e) {
            if (strstr(strtolower($e->getMessage()), 'could not find driver')) {
                throw new ApplicationException('Missing driver: ' . $e->getMessage());
            }
            throw new UserException('Error connecting to DB: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * @return PDO|mixed
     */
    abstract public function createConnection(array $params);

    abstract public function testConnection(): void;

    abstract public function simpleQuery(ExportConfig $export): string;

    abstract public function getMaxOfIncrementalFetchingColumn(ExportConfig $export): ?string;

    abstract public function getMetadataProvider(): MetadataProvider;

    public function getManifestMetadataSerializer(): ManifestSerializer
    {
        return new DefaultManifestSerializer();
    }

    public function getGetTablesMetadataSerializer(): GetTablesSerializer
    {
        return new DefaultGetTablesSerializer();
    }

    public function getTables(): array
    {
        $serializer = $this->getGetTablesMetadataSerializer();
        return $serializer->serialize($this->getMetadataProvider()->listTables());
    }

    public function validateIncrementalFetching(ExportConfig $export): void
    {
        throw new UserException('Incremental Fetching is not supported by this extractor.');
    }

    public function export(ExportConfig $export): array
    {
        if ($export->isIncremental()) {
            $this->validateIncrementalFetching($export);
            $maxValue = $this->canFetchMaxIncrementalValueSeparately($export) ?
                $this->getMaxOfIncrementalFetchingColumn($export) : null;
        } else {
            $maxValue = null;
        }

        $this->logger->info('Exporting to ' . $export->getOutputTable());
        $query = $export->hasQuery() ? $export->getQuery() : $this->simpleQuery($export);
        $proxy = new DbRetryProxy(
            $this->logger,
            $export->getMaxRetries(),
            [DeadConnectionException::class, ErrorException::class]
        );

        try {
            $result = $proxy->call(function () use ($query, $export) {
                /** @var PDOStatement $stmt */
                $stmt = $this->executeQuery($query, $export->getMaxRetries());
                $csv = $this->createOutputCsv($export->getOutputTable());
                $result = $this->writeToCsv($stmt, $csv, $export);
                $this->isAlive();
                return $result;
            });
        } catch (CsvException $e) {
            throw new ApplicationException('Failed writing CSV File: ' . $e->getMessage(), $e->getCode(), $e);
        } catch (\PDOException | \ErrorException | DeadConnectionException $e) {
            throw $this->handleDbError($e, $export);
        }

        if ($result['rows'] > 0) {
            $this->createManifest($export);
        } else {
            @unlink($this->getOutputFilename($export->getOutputTable())); // no rows, no file
            $this->logger->warning(sprintf(
                'Query returned empty result. Nothing was imported to [%s]',
                $export->getOutputTable()
            ));
        }

        $output = [
            'outputTable' => $export->getOutputTable(),
            'rows' => $result['rows'],
        ];

        // output state
        if ($export->isIncremental()) {
            if ($maxValue) {
                $output['state']['lastFetchedRow'] = $maxValue;
            } elseif (!empty($result['lastFetchedRow'])) {
                $output['state']['lastFetchedRow'] = $result['lastFetchedRow'];
            }
        }
        return $output;
    }

    protected function isAlive(): void
    {
        try {
            $this->testConnection();
        } catch (Throwable $e) {
            throw new DeadConnectionException('Dead connection: ' . $e->getMessage());
        }
    }

    protected function handleDbError(Throwable $e, ExportConfig $export): UserException
    {
        $message = sprintf('[%s]: ', $export->getOutputTable());
        $message .= sprintf('DB query failed: %s', $e->getMessage());

        // Retry mechanism can be disabled if maxRetries = 0
        if ($export->getMaxRetries() > 0) {
            $message .= sprintf(' Tried %d times.', $export->getMaxRetries());
        }

        return new UserException($message, 0, $e);
    }

    protected function executeQuery(string $query, ?int $maxTries): PDOStatement
    {
        $proxy = new DbRetryProxy($this->logger, $maxTries);

        $stmt = $proxy->call(function () use ($query) {
            try {
                /** @var \PDOStatement $stmt */
                $stmt = $this->db->prepare($query);
                $stmt->execute();
                return $stmt;
            } catch (Throwable $e) {
                try {
                    $this->db = $this->createConnection($this->dbParameters);
                } catch (Throwable $e) {
                };
                throw $e;
            }
        });
        return $stmt;
    }

    /**
     * @return array ['rows', 'lastFetchedRow']
     */
    protected function writeToCsv(PDOStatement $stmt, CsvWriter $csv, ExportConfig $export): array
    {
        // With custom query are no metadata in manifest, so header must be present
        $includeHeader = $export->hasQuery();
        $output = [];

        $resultRow = $stmt->fetch(PDO::FETCH_ASSOC);

        if (is_array($resultRow) && !empty($resultRow)) {
            // write header and first line
            if ($includeHeader) {
                $csv->writeRow(array_keys($resultRow));
            }
            $csv->writeRow($resultRow);

            // write the rest
            $numRows = 1;
            $lastRow = $resultRow;

            while ($resultRow = $stmt->fetch(PDO::FETCH_ASSOC)) {
                $csv->writeRow($resultRow);
                $lastRow = $resultRow;
                $numRows++;
            }
            $stmt->closeCursor();

            if ($export->isIncremental()) {
                $incrementalColumn = $export->getIncrementalFetchingConfig()->getColumn();
                if (!array_key_exists($incrementalColumn, $lastRow)) {
                    throw new UserException(
                        sprintf(
                            'The specified incremental fetching column %s not found in the table',
                            $incrementalColumn
                        )
                    );
                }
                $output['lastFetchedRow'] = $lastRow[$incrementalColumn];
            }
            $output['rows'] = $numRows;
            return $output;
        }
        // no rows found.  If incremental fetching is turned on, we need to preserve the last state
        if ($export->isIncremental() && isset($this->state['lastFetchedRow'])) {
            $output = $this->state;
        }
        $output['rows'] = 0;
        return $output;
    }

    protected function createOutputCsv(string $outputTable): CsvWriter
    {
        $outTablesDir = $this->dataDir . '/out/tables';
        if (!is_dir($outTablesDir)) {
            mkdir($outTablesDir, 0777, true);
        }
        return new CsvWriter($this->getOutputFilename($outputTable));
    }

    protected function createManifest(ExportConfig $export): void
    {
        $metadataSerializer = $this->getManifestMetadataSerializer();
        $outFilename = $this->getOutputFilename($export->getOutputTable()) . '.manifest';

        $manifestData = [
            'destination' => $export->getOutputTable(),
            'incremental' => $export->isIncremental(),
        ];

        if ($export->hasPrimaryKey()) {
            $manifestData['primary_key'] = $export->getPrimaryKey();
        }

        if (!$export->hasQuery()) {
            $table = $this->getMetadataProvider()->getTable($export->getTable());
            $allTableColumns = $table->getColumns();
            $columnMetadata = [];
            $sanitizedPks = [];
            $exportedColumns = $export->hasColumns() ? $export->getColumns() : $allTableColumns->getNames();
            foreach ($exportedColumns as $index => $columnName) {
                $column = $allTableColumns->getByName($columnName);
                if ($column->isPrimaryKey()) {
                    $sanitizedPks[] = $column->getSanitizedName();
                }

                $columnMetadata[$column->getSanitizedName()] = $metadataSerializer->serializeColumn($column);
            }

            $manifestData['metadata'] = $metadataSerializer->serializeTable($table);
            $manifestData['column_metadata'] = $columnMetadata;
            $manifestData['columns'] = array_keys($columnMetadata);
            if (!empty($sanitizedPks)) {
                $manifestData['primary_key'] = $sanitizedPks;
            }
        }

        file_put_contents($outFilename, json_encode($manifestData));
    }

    protected function getOutputFilename(string $outputTableName): string
    {
        $sanitizedTablename = Utils\Strings::webalize($outputTableName, '._');
        return $this->dataDir . '/out/tables/' . $sanitizedTablename . '.csv';
    }

    protected function getDbParameters(): array
    {
        return $this->dbParameters;
    }

    protected function canFetchMaxIncrementalValueSeparately(ExportConfig $export): bool
    {
        return !$export->hasQuery() && $export->isIncremental() && !$export->hasIncrementalLimit();
    }
}
