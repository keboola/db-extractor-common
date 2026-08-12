<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Fixtures\IncrementalFetchingWindow;

use Keboola\Component\Config\DatatypeSupport;
use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\Extractor\BaseExtractor;
use Keboola\DbExtractorConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Psr\Log\LoggerInterface;

/**
 * Test double for exercising BaseExtractor::export() and guardIncrementalFetchingWindow() without a real
 * database connection. Concrete subclasses only differ in whether they override
 * getIncrementalFetchingColumnType() (i.e. whether they "opt in" to the incremental fetching window).
 */
abstract class AbstractFakeExtractor extends BaseExtractor
{
    private FakeExportAdapter $fakeExportAdapter;

    public function __construct(array $parameters, array $state, LoggerInterface $logger)
    {
        // Must be ready before parent::__construct() runs, because it calls createExportAdapter().
        $this->fakeExportAdapter = new FakeExportAdapter();
        parent::__construct($parameters, $state, $logger, 'run', DatatypeSupport::HINTS);
    }

    public function testConnection(): void
    {
    }

    protected function createConnection(DatabaseConfig $databaseConfig): void
    {
    }

    protected function createExportAdapter(): ExportAdapter
    {
        return $this->fakeExportAdapter;
    }

    protected function createMetadataProvider(): MetadataProvider
    {
        return new FakeMetadataProvider();
    }

    protected function getMaxOfIncrementalFetchingColumn(ExportConfig $exportConfig): ?string
    {
        return null;
    }

    protected function validateIncrementalFetching(ExportConfig $exportConfig): void
    {
        // Assumed valid for these unit tests; the real metadata lookup is covered by the implementing
        // extractor's own tests (e.g. db-extractor-pgsql), not by db-extractor-common.
    }

    public function getCapturedExportConfig(): ?ExportConfig
    {
        return $this->fakeExportAdapter->getCapturedExportConfig();
    }

    /** Exposes the protected guard so tests can exercise it directly, in isolation from export(). */
    public function callGuardIncrementalFetchingWindow(ExportConfig $exportConfig): void
    {
        $this->guardIncrementalFetchingWindow($exportConfig);
    }
}
