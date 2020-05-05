<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Metadata\ValueObject;

use Keboola\DbExtractor\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\Metadata\ValueObject\ColumnCollection;
use Keboola\DbExtractor\Metadata\ValueObject\Table;
use Keboola\DbExtractor\ValueObject;
use Keboola\DbExtractor\Tests\ValueObject\BaseValueObjectTest;

class TableTest extends BaseValueObjectTest
{
    public function createValueObjectFromArray(array $properties): ValueObject
    {
        $name = $properties['name'];
        $sanitizedName = $properties['sanitizedName'];
        $description = $properties['description'];
        $schema = $properties['schema'];
        $catalog = $properties['catalog'];
        $type = $properties['type'];
        $rowCount = $properties['rowCount'];
        $column = $properties['columns'];
        return new Table(
            $name,
            $sanitizedName,
            $description,
            $schema,
            $catalog,
            $type,
            $rowCount,
            $column,
        );
    }

    public function getAllProperties(): array
    {
        return [
            'name',
            'sanitizedName',
            'description',
            'schema',
            'catalog',
            'type',
            'rowCount',
            'columns',
        ];
    }

    public function getNullableProperties(): array
    {
        return [
            // These properties CAN be set to null in Builder
            'description' => self::NULL_MEANS_NOT_SET,
            'schema' => self::NULL_MEANS_NOT_SET,
            'catalog' => self::NULL_MEANS_NOT_SET,
            'rowCount' => self::NULL_MEANS_NOT_SET,
            // These properties CANNOT be set to null in Builder
            // ... null value in constructor means "not set"
            'type' => self::NULL_MEANS_NOT_SET,
            'columns' => self::NULL_MEANS_NOT_SET,
        ];
    }

    public function getEmptyStringNotAllowedProperties(): array
    {
        return [
            'name',
            'sanitizedName',
        ];
    }

    public function getHasCallbacks(): array
    {
        return [
            'description' => function (Table $column) {
                return  $column->hasDescription();
            },
            'schema' => function (Table $column) {
                return  $column->hasSchema();
            },
            'catalog' => function (Table $column) {
                return  $column->hasCatalog();
            },
            'type' => function (Table $column) {
                return  $column->hasType();
            },
            'rowCount' => function (Table $column) {
                return  $column->hasRowCount();
            },
            'columns' => function (Table $column) {
                return  $column->hasColumns();
            },
        ];
    }

    public function getGetCallbacks(): array
    {
        return [
            'name' => function (Table $column) {
                return $column->getName();
            },
            'sanitizedName' => function (Table $column) {
                return $column->getSanitizedName();
            },
            'description' => function (Table $column) {
                return $column->getDescription();
            },
            'schema' => function (Table $column) {
                return $column->getSchema();
            },
            'catalog' => function (Table $column) {
                return $column->getCatalog();
            },
            'type' => function (Table $column) {
                return $column->getType();
            },
            'rowCount' => function (Table $column) {
                return $column->getRowCount();
            },
            'columns' => function (Table $column) {
                return $column->getColumns();
            },
        ];
    }

    public function getValidInputs(): array
    {
        return [
            [
                'name' => 'Table Name',
                'sanitizedName' => 'table_name',
                'description' => 'Some description',
                'schema' => 'my_schema',
                'catalog' => 'my_catalog',
                'type' => 'table',
                'rowCount' => 123,
                'columns' => new ColumnCollection([
                    ColumnBuilder::create()
                        ->setOrdinalPosition(0)->setName('Col 1')->setType('integer')->build(),
                    ColumnBuilder::create()
                        ->setOrdinalPosition(1)->setName('Col 2')->setType('integer')->build(),
                ]),
            ],
        ];
    }
}
