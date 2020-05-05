<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Metadata\Manifest;

use Keboola\Datatype\Definition\GenericStorage;
use Keboola\DbExtractor\Metadata\Manifest\ManifestSerializer;
use Keboola\DbExtractor\Metadata\ValueObject\Column;
use Keboola\DbExtractor\Metadata\ValueObject\Table;

class DefaultManifestSerializer implements ManifestSerializer
{
    public function serializeTable(Table $table): array
    {
        $values = [
            'name' => $table->getName(),
            'sanitizedName' => $table->getSanitizedName(),
            'description' => $table->hasDescription() ? $table->getDescription() : null,
            'schema' => $table->hasSchema() ? $table->getSchema() : null,
            'catalog' => $table->hasCatalog() ? $table->getCatalog() : null,
            'type' => $table->hasType() ? $table->getType() : null,
            'rowCount' => $table->hasRowCount() ? $table->getRowCount() : null,
        ];

        $metadata = [];
        foreach ($values as $key => $value) {
            if ($value === null) {
                // Skip null value
                continue;
            }

            $metadata[] = [
                'key' => 'KBC.' . $key,
                'value' => $value,
            ];
        }

        return $metadata;
    }

    public function serializeColumn(Column $column): array
    {
        // Datatype metadata
        $options = [
            'type' => $column->getType(),
            'length' => $column->hasLength() ? $column->getLength() : null,
            'nullable' => $column->isNullable(),
            'default' => $column->hasDefault() ? (string) $column->getDefault() : null,
        ];
        $options = array_filter($options, fn($value) => $value !== null); // remove null values
        $datatype = new GenericStorage($column->getType(), $options);
        $columnMetadata = $datatype->toMetadata();

        // Non-datatype metadata
        $nonDatatypeMetadata = [
            'sourceName' => $column->getName(),
            'sanitizedName' => $column->getSanitizedName(),
            'primaryKey' => $column->isPrimaryKey(),
            'ordinalPosition' => $column->hasOrdinalPosition() ? $column->getOrdinalPosition() : null,
        ];

        foreach ($nonDatatypeMetadata as $key => $value) {
            if ($value === null) {
                // Skip null value
                continue;
            }

            $columnMetadata[] = [
                'key' => 'KBC.' . $key,
                'value' => $value,
            ];
        }

        // Constraints
        foreach ($column->getConstraints() as $constraint) {
            $columnMetadata[] = [
                'key' => 'KBC.constraintName',
                'value' => $constraint,
            ];
        }

        return $columnMetadata;
    }
}
