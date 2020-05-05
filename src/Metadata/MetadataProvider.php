<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Metadata;

use Keboola\DbExtractor\Configuration\InputTable;
use Keboola\DbExtractor\Metadata\ValueObject\Table;
use Keboola\DbExtractor\Metadata\ValueObject\TableCollection;

interface MetadataProvider
{
    public function getTable(InputTable $table): Table;

    /**
     * @param array|InputTable[] $whitelist
     * @param bool $loadColumns if false, columns metadata are NOT loaded, useful if there are a lot of tables
     */
    public function listTables(array $whitelist = [], bool $loadColumns = true): TableCollection;
}
