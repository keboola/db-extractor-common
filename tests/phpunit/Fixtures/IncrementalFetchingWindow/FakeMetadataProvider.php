<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Fixtures\IncrementalFetchingWindow;

use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\Table;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\TableCollection;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;
use LogicException;

/**
 * Test double. The unit tests always produce a fake export result with a CSV header present, so
 * DefaultManifestGenerator never needs real table metadata; getTable() is intentionally unimplemented.
 */
class FakeMetadataProvider implements MetadataProvider
{
    public function getTable(InputTable $table): Table
    {
        throw new LogicException('Not implemented in this test double.');
    }

    public function listTables(array $whitelist = [], bool $loadColumns = true): TableCollection
    {
        return new TableCollection([]);
    }
}
