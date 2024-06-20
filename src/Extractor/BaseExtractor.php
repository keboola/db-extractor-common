<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Component\Config\DatatypeSupport;
use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Manifest\DefaultManifestGenerator;
use Keboola\DbExtractor\Manifest\ManifestGenerator;
use Keboola\DbExtractor\TableResultFormat\Metadata\GetTables\DefaultGetTablesSerializer;
use Keboola\DbExtractor\TableResultFormat\Metadata\GetTables\GetTablesSerializer;
use Keboola\DbExtractor\TableResultFormat\Metadata\Manifest\DefaultManifestSerializer;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;
use Keboola\DbExtractorSSHTunnel\Exception\UserException as SSHTunnelUserException;
use Keboola\DbExtractorSSHTunnel\SSHTunnel;
use Nette\Utils;
use Psr\Log\LoggerInterface;

abstract class BaseExtractor
{
    protected array $state;

    protected string $dataDir;

    protected array $parameters;

    protected LoggerInterface $logger;

    protected ExportAdapter $adapter;

    protected MetadataProvider $metadataProvider;

    protected GetTablesSerializer $getTablesSerializer;

    protected ManifestGenerator $manifestGenerator;

    private bool $syncAction;

    private DatabaseConfig $databaseConfig;

    private DatatypeSupport $datatypeSupport;

    public function __construct(
        array $parameters,
        array $state,
        LoggerInterface $logger,
        string $action,
        DatatypeSupport $datatypeSupport,
    ) {
        $this->parameters = $parameters;
        $this->dataDir = $parameters['data_dir'];
        $this->state = $state;
        $this->logger = $logger;
        $this->parameters = $this->createSshTunnel($this->parameters);
        $this->syncAction = $action !== 'run';

        $this->databaseConfig = $this->createDatabaseConfig($this->parameters['db']);
        $this->createConnection($this->databaseConfig);
        $this->metadataProvider = $this->createMetadataProvider();
        $this->getTablesSerializer = $this->createGetTablesSerializer();
        $this->manifestGenerator = $this->createManifestGenerator();
        $this->adapter = $this->createExportAdapter();
        $this->datatypeSupport = $datatypeSupport;
    }

    abstract public function testConnection(): void;

    abstract protected function createConnection(DatabaseConfig $databaseConfig): void;

    abstract protected function createExportAdapter(): ExportAdapter;

    abstract protected function createMetadataProvider(): MetadataProvider;

    abstract protected function getMaxOfIncrementalFetchingColumn(ExportConfig $exportConfig): ?string;

    public function getMetadataProvider(): MetadataProvider
    {
        return $this->metadataProvider;
    }

    public function getGetTablesSerializer(): GetTablesSerializer
    {
        return $this->getTablesSerializer;
    }

    public function getManifestGenerator(): ManifestGenerator
    {
        return $this->manifestGenerator;
    }

    protected function createManifestGenerator(): ManifestGenerator
    {
        return new DefaultManifestGenerator(
            $this->getMetadataProvider(),
            new DefaultManifestSerializer(),
        );
    }

    protected function createGetTablesSerializer(): GetTablesSerializer
    {
        return new DefaultGetTablesSerializer();
    }

    protected function validateIncrementalFetching(ExportConfig $exportConfig): void
    {
        throw new UserException('Incremental Fetching is not supported by this extractor.');
    }

    public function getTables(): array
    {
        $loadColumns = $this->parameters['tableListFilter']['listColumns'] ?? true;
        $whiteList = array_map(
            function (array $table) {
                return new InputTable($table['tableName'], $table['schema']);
            },
            $this->parameters['tableListFilter']['tablesToList'] ?? [],
        );

        $tables = $this->getMetadataProvider()->listTables($whiteList, $loadColumns);
        return $this->getGetTablesSerializer()->serialize($tables);
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
        $this->createManifest($exportConfig, $result, $this->datatypeSupport->usingLegacyManifest());
        if ($result->getRowsCount() > 0) {
            $this->logger->info(sprintf(
                'Exported "%d" rows to "%s".',
                $result->getRowsCount(),
                $exportConfig->getOutputTable(),
            ));
        } else {
            $this->logger->warning(sprintf(
                'Query result set is empty. Exported "0" rows to "%s".',
                $exportConfig->getOutputTable(),
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

    protected function createManifest(ExportConfig $exportConfig, ExportResult $exportResult, bool $legacy): void
    {
        $outFilename = $this->getOutputFilename($exportConfig->getOutputTable()) . '.manifest';
        $manifestData = $this->manifestGenerator->generate($exportConfig, $exportResult, $legacy);
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

    protected function isSyncAction(): bool
    {
        return $this->syncAction;
    }
}
