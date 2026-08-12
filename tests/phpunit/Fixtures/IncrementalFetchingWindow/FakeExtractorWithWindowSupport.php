<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Fixtures\IncrementalFetchingWindow;

use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Psr\Log\LoggerInterface;

/**
 * Simulates an extractor that HAS opted in to the incremental fetching window feature by overriding
 * getIncrementalFetchingColumnType(). The returned basetype is configurable so tests can cover both the
 * TIMESTAMP and numeric (INTEGER/NUMERIC/FLOAT) paths.
 */
class FakeExtractorWithWindowSupport extends AbstractFakeExtractor
{
    public function __construct(
        array $parameters,
        array $state,
        LoggerInterface $logger,
        private readonly string $columnType = 'TIMESTAMP',
    ) {
        parent::__construct($parameters, $state, $logger);
    }

    protected function getIncrementalFetchingColumnType(ExportConfig $exportConfig): ?string
    {
        return $this->columnType;
    }
}
