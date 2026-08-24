<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use DateTimeImmutable;
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
use Keboola\DbExtractorConfig\Incremental\WindowBoundResolver;
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
            $this->parameters['extractor_class'],
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

    /**
     * Returns the basetype (e.g. "TIMESTAMP", "INTEGER", "NUMERIC", "FLOAT") of the incremental fetching
     * column, so the incremental fetching WINDOW feature can resolve relative/absolute bounds against it.
     *
     * Default is `null`, meaning the window feature is unavailable for this extractor. An extractor opts
     * in by overriding this method and returning the detected basetype (typically reusing the same
     * metadata lookup already performed in validateIncrementalFetching()).
     */
    protected function getIncrementalFetchingColumnType(ExportConfig $exportConfig): ?string
    {
        return null;
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

            // Window mode ignores the watermark; with no start/end it would degrade to a full-table scan
            // every run. Fail loudly instead. (hasIncrementalFetchingBounds() is false in this case, so
            // the guard below would not otherwise fire.)
            if ($exportConfig->isIncrementalFetchingWindowMode() && !$exportConfig->hasIncrementalFetchingWindow()) {
                throw new UserException(
                    'Incremental fetching "window" mode requires at least one of "incrementalFetchingStart" ' .
                    'or "incrementalFetchingEnd" to be set.',
                );
            }

            if ($exportConfig->hasIncrementalFetchingBounds()) {
                $columnType = $this->getIncrementalFetchingColumnType($exportConfig);
                if ($columnType === null) {
                    throw new UserException(
                        'Incremental fetching window/lookback is not supported by this extractor.',
                    );
                }
                $exportConfig = $exportConfig->withIncrementalColumnType($columnType);
                $this->guardIncrementalFetchingOverlap($exportConfig);
            }

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

    /**
     * Guards for the incremental fetching WINDOW and watermark LOOKBACK features. No-op unless one of
     * them is actually configured.
     *
     * 1) A window "start" or a watermark "lookback" both re-emit rows that may already be in Storage.
     *    Combined with incremental LOADING (append) and no primary key, that produces duplicate rows
     *    because there is nothing to deduplicate on. This is a hard error.
     * 2) An absolute window "end" caps the fetched range at a fixed point in time; rows committed after
     *    it will never be picked up by subsequent incremental runs. That's expected for a one-off or
     *    segmented backfill, but easy to set by mistake on an otherwise-recurring config, so it's only
     *    a warning. (Only applies in window mode.)
     * 3) A fetch "limit" combined with a window or a lookback either never advances through the bounded
     *    range (window returns the same first page) or moves the watermark backwards (lookback), so newer
     *    rows are never reached. This is a hard error. (Plain watermark+limit chunking is unaffected.)
     *
     * Expects $exportConfig to already carry a resolved incremental column type
     * (see ExportConfig::withIncrementalColumnType()).
     */
    protected function guardIncrementalFetchingOverlap(ExportConfig $exportConfig): void
    {
        // A fetch limit caps the ascending result to its first N rows. Combined with a window it keeps
        // returning the first page of a fixed range and never advances; combined with a lookback it
        // persists an older row as the watermark and moves it backwards. Reject the limit for any bounds.
        // (This guard only runs when bounds are configured, so the limit alone is the condition here;
        // plain watermark+limit chunking never reaches this method.)
        if ($exportConfig->hasIncrementalFetchingLimit()) {
            throw new UserException(
                'Incremental fetching "incrementalFetchingLimit" cannot be combined with a window or a ' .
                'lookback: the limited result never advances through the bounded range (window) or moves ' .
                'the watermark backwards (lookback), so newer rows are never reached. Remove the limit, ' .
                'or use plain watermark mode.',
            );
        }

        $reFetchesOverlap = $exportConfig->hasIncrementalFetchingLookback()
            || ($exportConfig->hasIncrementalFetchingWindow()
                && $exportConfig->getIncrementalFetchingWindowStart() !== null);

        if ($reFetchesOverlap
            && $exportConfig->isIncrementalLoading()
            && !$exportConfig->hasPrimaryKey()
        ) {
            throw new UserException(
                'Incremental fetching lookback/window "start" can re-fetch rows already loaded to storage. ' .
                'A primary key is required on the table so that incremental loading can deduplicate them.',
            );
        }

        if (!$exportConfig->hasIncrementalFetchingWindow()) {
            return;
        }

        $windowEnd = $exportConfig->getIncrementalFetchingWindowEnd();
        $columnType = $exportConfig->getIncrementalColumnType();
        if ($windowEnd !== null && $this->isAbsoluteWindowBound($windowEnd, $columnType)) {
            $this->logger->warning(
                'Incremental fetching window "end" is set to an absolute value. Rows committed after ' .
                'this point in time will never be fetched by future incremental runs. This is expected ' .
                'for a one-off or segmented backfill, but not for ongoing incremental synchronization.',
            );
        }
    }

    /**
     * A window bound is "absolute" when it resolves to the same literal regardless of when "now" is
     * evaluated (e.g. "2026-01-01"), as opposed to "relative" (e.g. "20 minutes ago", "now"), which
     * moves with the wall clock. Probing the resolver at two different instants of "now" tells them
     * apart without having to parse the raw string ourselves.
     */
    private function isAbsoluteWindowBound(string $rawValue, string $columnType): bool
    {
        $resolver = new WindowBoundResolver();
        $now = new DateTimeImmutable();
        $laterNow = $now->modify('+1 day');

        return $resolver->resolveUpperBound($rawValue, $columnType, $now)
            === $resolver->resolveUpperBound($rawValue, $columnType, $laterNow);
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
