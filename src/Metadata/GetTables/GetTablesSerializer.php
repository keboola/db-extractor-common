<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Metadata\GetTables;

use Keboola\DbExtractor\Metadata\ValueObject\TableCollection;

interface GetTablesSerializer
{
    public function serialize(TableCollection $tables): array;
}
