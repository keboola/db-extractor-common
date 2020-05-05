<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Metadata\GetTables;

use Keboola\DbExtractor\Metadata\ValueObject\Column;
use Keboola\DbExtractor\Metadata\ValueObject\Table;
use Keboola\DbExtractor\Metadata\ValueObject\TableCollection;

class DefaultGetTablesSerializer implements GetTablesSerializer
{
    public function serialize(TableCollection $tables): array
    {
        return array_map(
            fn(Table $table) => $this->serializeTable($table),
            $tables->getAll()
        );
    }

    protected function serializeTable(Table $table): array
    {
        $out = [];
        $out['name'] = $table->getName();
        $out['schema'] = $table->getSchema();

        // Columns don't have to be defined if loading metadata in 2 steps:
        // first only tables and then columns in table (used in some extractors)
        if ($table->hasColumns()) {
            $out['columns'] = array_map(
                fn(Column $col) => $this->serializeColumn($col),
                $table->getColumns()->getAll()
            );
        }

        return $out;
    }

    protected function serializeColumn(Column $column): array
    {
        return [
            'name' => $column->getName(),
            'type' => $column->getType(),
            'primaryKey' => $column->isPrimaryKey(),
        ];
    }
}
