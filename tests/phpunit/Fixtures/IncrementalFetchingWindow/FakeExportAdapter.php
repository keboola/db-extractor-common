<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Fixtures\IncrementalFetchingWindow;

use Keboola\DbExtractor\Adapter\ExportAdapter;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

/**
 * Test double that records the ExportConfig it receives instead of running a real export, so tests can
 * assert exactly what BaseExtractor::export() threaded into the adapter (e.g. the resolved incremental
 * column type via ExportConfig::withIncrementalColumnType()).
 */
class FakeExportAdapter implements ExportAdapter
{
    private ?ExportConfig $capturedExportConfig = null;

    public function getName(): string
    {
        return 'fake';
    }

    public function export(ExportConfig $exportConfig, string $csvFilePath): ExportResult
    {
        $this->capturedExportConfig = $exportConfig;
        touch($csvFilePath);

        // rowsCount=1 (not 0) so BaseExtractor::processExportResult() doesn't log its own "empty result"
        // warning, which would otherwise pollute this test suite's assertions about OUR warnings.
        // hasCsvHeader=true so DefaultManifestGenerator skips reading (fake) table/query metadata.
        return new ExportResult($csvFilePath, 1, new FakeQueryMetadata(), true, null);
    }

    public function getCapturedExportConfig(): ?ExportConfig
    {
        return $this->capturedExportConfig;
    }
}
