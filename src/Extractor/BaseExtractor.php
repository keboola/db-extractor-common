<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Throwable;
use Psr\Log\LoggerInterface;
use Nette\Utils;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;
use Keboola\DbExtractor\TableResultFormat\Metadata\GetTables\DefaultGetTablesSerializer;
use Keboola\DbExtractor\TableResultFormat\Metadata\GetTables\GetTablesSerializer;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\TableResultFormat\Metadata\Manifest\DefaultManifestSerializer;
use Keboola\DbExtractor\TableResultFormat\Metadata\Manifest\ManifestSerializer;
use Keboola\DbExtractorSSHTunnel\SSHTunnel;
use Keboola\DbExtractorSSHTunnel\Exception\UserException as SSHTunnelUserException;

abstract class BaseExtractor
{
    protected array $state;

    protected string $dataDir;

    protected array $parameters;

    protected LoggerInterface $logger;

    protected ExportAdapter $adapter;

    private DatabaseConfig $databaseConfig;

    public function __construct(array $parameters, array $state, LoggerInterface $logger)
    {
        $this->parameters = $parameters;
        $this->dataDir = $parameters['data_dir'];
        $this->state = $state;
        $this->logger = $logger;
        $this->parameters = $this->createSshTunnel($this->parameters);
        $this->databaseConfig = $this->createDatabaseConfig($parameters['db']);
        $this->createConnection($this->databaseConfig);
        $this->adapter = $this->createExportAdapter();
    }

    abstract public function testConnection(): void;

    abstract protected function createConnection(DatabaseConfig $databaseConfig): void;

    abstract protected function createExportAdapter(): ExportAdapter;

    abstract protected function getMetadataProvider(): MetadataProvider;

    abstract protected function getMaxOfIncrementalFetchingColumn(ExportConfig $exportConfig): ?string;

    protected function validateIncrementalFetching(ExportConfig $exportConfig): void
    {
        throw new UserException('Incremental Fetching is not supported by this extractor.');
    }

    protected function getManifestMetadataSerializer(): ManifestSerializer
    {
        return new DefaultManifestSerializer();
    }

    protected function getGetTablesMetadataSerializer(): GetTablesSerializer
    {
        return new DefaultGetTablesSerializer();
    }

    public function getTables(): array
    {
        $loadColumns = $this->parameters['tableListFilter']['listColumns'] ?? true;
        $whiteList = array_map(
            function (array $table) {
                return new InputTable($table['tableName'], $table['schema']);
            },
            $this->parameters['tableListFilter']['tablesToList'] ?? []
        );

        $serializer = $this->getGetTablesMetadataSerializer();
        return $serializer->serialize($this->getMetadataProvider()->listTables($whiteList, $loadColumns));
    }

    public function export(ExportConfig $exportConfig): array
    {
        if ($exportConfig->isIncrementalFetching()) {
            $this->validateIncrementalFetching($exportConfig);
            $maxValue = $this->canFetchMaxIncrementalValueSeparately($exportConfig) ?
                $this->getMaxOfIncrementalFetchingColumn($exportConfig) : null;
        } else {
            $maxValue = null;
        }

        $this->logger->info($exportConfig->hasConfigName() ?
            sprintf('Exporting "%s" to "%s".', $exportConfig->getConfigName(), $exportConfig->getOutputTable()) :
            sprintf('Exporting to "%s".', $exportConfig->getOutputTable()));
        $csvFilePath = $this->getOutputFilename($exportConfig->getOutputTable());
        $result = $this->adapter->export($exportConfig, $csvFilePath);
        return $this->processExportResult($exportConfig, $maxValue, $result);
    }

    protected function processExportResult(ExportConfig $exportConfig, ?string $maxValue, ExportResult $result): array
    {
        if ($result->getRowsCount() > 0) {
            $this->createManifest($exportConfig);
        } else {
            @unlink($this->getOutputFilename($exportConfig->getOutputTable())); // no rows, no file
            $this->logger->warning(sprintf(
                'Query returned empty result. Nothing was imported to "%s".',
                $exportConfig->getOutputTable()
            ));
        }

        $output = [
            'outputTable' => $exportConfig->getOutputTable(),
            'rows' => $result->getRowsCount(),
        ];

        // output state
        if ($exportConfig->isIncrementalFetching()) {
            if ($maxValue) {
                $output['state']['lastFetchedRow'] = $maxValue;
            } elseif (!empty($result->getIncFetchingColMaxValue())) {
                $output['state']['lastFetchedRow'] = $result->getIncFetchingColMaxValue();
            }
        }
        return $output;
    }

    protected function createManifest(ExportConfig $exportConfig): void
    {
        $metadataSerializer = $this->getManifestMetadataSerializer();
        $outFilename = $this->getOutputFilename($exportConfig->getOutputTable()) . '.manifest';

        $manifestData = [
            'destination' => $exportConfig->getOutputTable(),
            'incremental' => $exportConfig->isIncrementalLoading(),
        ];

        if ($exportConfig->hasPrimaryKey()) {
            $manifestData['primary_key'] = $exportConfig->getPrimaryKey();
        }

        if (!$exportConfig->hasQuery()) {
            $table = $this->getMetadataProvider()->getTable($exportConfig->getTable());
            $allTableColumns = $table->getColumns();
            $columnMetadata = [];
            $sanitizedPks = [];
            $exportedColumns = $exportConfig->hasColumns() ? $exportConfig->getColumns() : $allTableColumns->getNames();
            foreach ($exportedColumns as $index => $columnName) {
                $column = $allTableColumns->getByName($columnName);
                $columnMetadata[$column->getSanitizedName()] = $metadataSerializer->serializeColumn($column);

                // Sanitize PKs defined in the configuration
                if ($exportConfig->hasPrimaryKey() &&
                    in_array($column->getName(), $exportConfig->getPrimaryKey(), true)
                ) {
                    $sanitizedPks[] = $column->getSanitizedName();
                }
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
        $sanitizedTableName = Utils\Strings::webalize($outputTableName, '._');
        $outTablesDir = $this->dataDir . '/out/tables';
        return $outTablesDir . '/' . $sanitizedTableName . '.csv';
    }

    protected function getDatabaseConfig(): DatabaseConfig
    {
        return $this->databaseConfig;
    }

    protected function canFetchMaxIncrementalValueSeparately(ExportConfig $exportConfig): bool
    {
        return
            !$exportConfig->hasQuery() &&
            $exportConfig->isIncrementalFetching() &&
            !$exportConfig->hasIncrementalFetchingLimit();
    }

    protected function createSshTunnel(array $parameters): array
    {
        if (isset($parameters['db']['ssh']['enabled']) && $parameters['db']['ssh']['enabled']) {
            try {
                $sshTunnel = new SSHTunnel($this->logger);
                $parameters['db'] = $sshTunnel->createSshTunnel($parameters['db']);
            } catch (SSHTunnelUserException $e) {
                throw new UserException($e->getMessage(), 0, $e);
            }
        }
        return $parameters;
    }

    protected function createDatabaseConfig(array $data): DatabaseConfig
    {
        return DatabaseConfig::fromArray($data);
    }
}
