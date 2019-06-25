<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Csv\CsvFile;
use Keboola\Csv\Exception as CsvException;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\DeadConnectionException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorLogger\Logger;
use Keboola\DbExtractorSSHTunnel\SSHTunnel;
use Keboola\DbExtractorSSHTunnel\Exception\UserException as SSHTunnelUserException;
use Nette\Utils;

use PDOException;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;
use Throwable;
use PDO;
use PDOStatement;

abstract class Extractor
{
    public const DEFAULT_MAX_TRIES = 25;

    public const DATATYPE_KEYS = ['type', 'length', 'nullable', 'default', 'format'];

    /** @var PDO|mixed */
    protected $db;

    /** @var  array */
    protected $state;

    /** @var  array|null with keys type (autoIncrement or timestamp), column, and limit */
    protected $incrementalFetching;

    /** @var Logger */
    protected $logger;

    /** @var string */
    protected $dataDir;

    /** @var array */
    private $dbParameters;

    public function __construct(array $parameters, array $state = [], ?Logger $logger = null)
    {
        if (is_null($logger)) {
            $logger = new Logger('db-ex-common');
        }
        $this->logger = $logger;
        $this->dataDir = $parameters['data_dir'];
        $this->state = $state;

        if (isset($parameters['db']['ssh']['enabled']) && $parameters['db']['ssh']['enabled']) {
            try {
                $sshTunnel = new SSHTunnel($logger);
                $parameters['db'] = $sshTunnel->createSshTunnel($parameters['db']);
            } catch (SSHTunnelUserException $e) {
                throw new UserException($e->getMessage());
            }
        }
        $this->dbParameters = $parameters['db'];

        $simplyRetryPolicy = new SimpleRetryPolicy(
            self::DEFAULT_MAX_TRIES,
            [PDOException::class]
        );
        $exponentialBackOffPolicy = new ExponentialBackOffPolicy();

        $proxy = new RetryProxy(
            $simplyRetryPolicy,
            $exponentialBackOffPolicy,
            $this->logger
        );

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
        if (isset($parameters['incrementalFetchingColumn']) && $parameters['incrementalFetchingColumn'] !== '') {
            $this->validateIncrementalFetching(
                $parameters['table'],
                $parameters['incrementalFetchingColumn'],
                isset($parameters['incrementalFetchingLimit']) ? $parameters['incrementalFetchingLimit'] : null
            );
        }
    }

    /**
     * @param array $params
     * @return PDO|mixed
     */
    abstract public function createConnection(array $params);

    /**
     * @return void|mixed
     */
    abstract public function testConnection();

    /**
     * @param array|null $tables - an optional array of tables with tableName and schema properties
     */
    abstract public function getTables(?array $tables = null): array;

    abstract public function simpleQuery(array $table, array $columns = array()): string;

    /**
     * @param array $table
     * @param string $columnName
     * @param int|null $limit
     * @throws UserException
     */
    public function validateIncrementalFetching(array $table, string $columnName, ?int $limit = null): void
    {
        throw new UserException('Incremental Fetching is not supported by this extractor.');
    }

    public function export(array $table): array
    {
        $outputTable = $table['outputTable'];

        $this->logger->info('Exporting to ' . $outputTable);

        $isAdvancedQuery = true;
        if (array_key_exists('table', $table) && !array_key_exists('query', $table)) {
            $isAdvancedQuery = false;
            $query = $this->simpleQuery($table['table'], $table['columns']);
        } else {
            $query = $table['query'];
        }
        $maxTries = isset($table['retries']) ? (int) $table['retries'] : self::DEFAULT_MAX_TRIES;

        $simplyRetryPolicy = new SimpleRetryPolicy(
            $maxTries,
            [DeadConnectionException::class, \ErrorException::class]
        );
        $exponentialBackOffPolicy = new ExponentialBackOffPolicy();

        $proxy = new RetryProxy(
            $simplyRetryPolicy,
            $exponentialBackOffPolicy,
            $this->logger
        );

        try {
            $result = $proxy->call(function () use ($query, $maxTries, $outputTable, $isAdvancedQuery) {
                /** @var PDOStatement $stmt */
                $stmt = $this->executeQuery($query, $maxTries);
                $csv = $this->createOutputCsv($outputTable);
                $result = $this->writeToCsv($stmt, $csv, $isAdvancedQuery);
                $this->isAlive();
                return $result;
            });
        } catch (CsvException $e) {
            throw new ApplicationException('Failed writing CSV File: ' . $e->getMessage(), $e->getCode(), $e);
        } catch (\PDOException $e) {
            throw $this->handleDbError($e, $table, $maxTries);
        } catch (\ErrorException $e) {
            throw $this->handleDbError($e, $table, $maxTries);
        } catch (DeadConnectionException $e) {
            throw $this->handleDbError($e, $table, $maxTries);
        }
        if ($result['rows'] > 0) {
            $this->createManifest($table);
        } else {
            $this->logger->warn(
                sprintf(
                    'Query returned empty result. Nothing was imported to [%s]',
                    $table['outputTable']
                )
            );
        }

        $output = [
            'outputTable' => $outputTable,
            'rows' => $result['rows'],
        ];
        // output state
        if (!empty($result['lastFetchedRow'])) {
            $output['state']['lastFetchedRow'] = $result['lastFetchedRow'];
        }
        return $output;
    }

    protected function isAlive(): void
    {
        try {
            $this->testConnection();
        } catch (\Throwable $e) {
            throw new DeadConnectionException('Dead connection: ' . $e->getMessage());
        }
    }

    protected function handleDbError(Throwable $e, ?array $table = null, ?int $counter = null): UserException
    {
        $message = '';
        if ($table) {
            $message = sprintf('[%s]: ', $table['outputTable']);
        }
        $message .= sprintf('DB query failed: %s', $e->getMessage());
        if ($counter) {
            $message .= sprintf(' Tried %d times.', $counter);
        }
        return new UserException($message, 0, $e);
    }

    protected function executeQuery(string $query, ?int $maxTries): PDOStatement
    {
        $simplyRetryPolicy = new SimpleRetryPolicy($maxTries);
        $exponentialBackOffPolicy = new ExponentialBackOffPolicy();

        $proxy = new RetryProxy(
            $simplyRetryPolicy,
            $exponentialBackOffPolicy,
            $this->logger
        );

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
     * @param PDOStatement $stmt
     * @param CsvFile $csv
     * @param boolean $includeHeader
     * @return array ['rows', 'lastFetchedRow']
     */
    protected function writeToCsv(PDOStatement $stmt, CsvFile $csv, bool $includeHeader = true): array
    {
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

            if (isset($this->incrementalFetching['column'])) {
                if (!array_key_exists($this->incrementalFetching['column'], $lastRow)) {
                    throw new UserException(
                        sprintf(
                            'The specified incremental fetching column %s not found in the table',
                            $this->incrementalFetching['column']
                        )
                    );
                }
                $output['lastFetchedRow'] = $lastRow[$this->incrementalFetching['column']];
            }
            $output['rows'] = $numRows;
            return $output;
        }
        // no rows found.  If incremental fetching is turned on, we need to preserve the last state
        if ($this->incrementalFetching['column'] && isset($this->state['lastFetchedRow'])) {
            $output = $this->state;
        }
        $output['rows'] = 0;
        return $output;
    }

    protected function createOutputCsv(string $outputTable): CsvFile
    {
        $outTablesDir = $this->dataDir . '/out/tables';
        if (!is_dir($outTablesDir)) {
            mkdir($outTablesDir, 0777, true);
        }
        return new CsvFile($this->getOutputFilename($outputTable));
    }

    /**
     * @param array $table
     * @return bool|int
     */
    protected function createManifest(array $table)
    {
        $outFilename = $this->getOutputFilename($table['outputTable']) . '.manifest';

        $manifestData = [
            'destination' => $table['outputTable'],
            'incremental' => $table['incremental'],
        ];

        if (!empty($table['primaryKey'])) {
            $manifestData['primary_key'] = $table['primaryKey'];
        }

        $manifestColumns = [];

        if (isset($table['table']) && !is_null($table['table'])) {
            $tables = $this->getTables([$table['table']]);
            if (count($tables) > 0) {
                $tableDetails = $tables[0];
                $columnMetadata = [];
                $sanitizedPks = [];
                $iterColumns = $table['columns'];
                if (count($iterColumns) === 0) {
                    $iterColumns = array_map(function ($column) {
                        return $column['name'];
                    }, $tableDetails['columns']);
                }
                foreach ($iterColumns as $ind => $columnName) {
                    $column = null;
                    foreach ($tableDetails['columns'] as $detailColumn) {
                        if ($detailColumn['name'] === $columnName) {
                            $column = $detailColumn;
                        }
                    }
                    if (!$column) {
                        throw new UserException(
                            sprintf('The given column \'%s\' was not found in the table.', $columnName)
                        );
                    }
                    // use sanitized name for primary key if available
                    if (in_array($column['name'], $table['primaryKey']) && array_key_exists('sanitizedName', $column)) {
                        $sanitizedPks[] = $column['sanitizedName'];
                    }
                    $columnName = $column['name'];
                    if (array_key_exists('sanitizedName', $column)) {
                        $columnName = $column['sanitizedName'];
                    }
                    $columnMetadata[$columnName] = $this->getColumnMetadata($column);
                    $manifestColumns[] = $columnName;
                }
                $manifestData['metadata'] = $this->getTableLevelMetadata($tableDetails);

                $manifestData['column_metadata'] = $columnMetadata;
                $manifestData['columns'] = $manifestColumns;
                if (!empty($sanitizedPks)) {
                    $manifestData['primary_key'] = $sanitizedPks;
                }
            }
        }
        return file_put_contents($outFilename, json_encode($manifestData));
    }

    public static function getColumnMetadata(array $column): array
    {
        $datatype = new GenericStorage(
            $column['type'],
            array_intersect_key($column, array_flip(self::DATATYPE_KEYS))
        );
        $columnMetadata = $datatype->toMetadata();
        $nonDatatypeKeys = array_diff_key($column, array_flip(self::DATATYPE_KEYS));
        foreach ($nonDatatypeKeys as $key => $value) {
            if ($key === 'name') {
                $columnMetadata[] = [
                    'key' => 'KBC.sourceName',
                    'value' => $value,
                ];
            } else {
                $columnMetadata[] = [
                    'key' => 'KBC.' . $key,
                    'value' => $value,
                ];
            }
        }
        return $columnMetadata;
    }

    public static function getTableLevelMetadata(array $tableDetails): array
    {
        $metadata = [];
        foreach ($tableDetails as $key => $value) {
            if ($key === 'columns') {
                continue;
            }
            $metadata[] = [
                'key' => 'KBC.' . $key,
                'value' => $value,
            ];
        }
        return $metadata;
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
}
