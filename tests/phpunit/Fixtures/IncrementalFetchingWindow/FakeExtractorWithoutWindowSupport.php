<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Fixtures\IncrementalFetchingWindow;

/**
 * Simulates an extractor that has NOT opted in to the incremental fetching window feature: it does not
 * override getIncrementalFetchingColumnType(), so it inherits BaseExtractor's default (null).
 */
class FakeExtractorWithoutWindowSupport extends AbstractFakeExtractor
{
}
