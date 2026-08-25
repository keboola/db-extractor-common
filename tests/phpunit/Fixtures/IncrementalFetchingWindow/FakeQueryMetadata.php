<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Fixtures\IncrementalFetchingWindow;

use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;

/**
 * Test double. Every fake export in these tests reports a CSV header (see FakeExportAdapter), so the
 * manifest generator never actually reads these columns; kept empty on purpose.
 */
class FakeQueryMetadata implements QueryMetadata
{
    public function getColumns(): ColumnCollection
    {
        return new ColumnCollection([]);
    }
}
