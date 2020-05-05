<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Metadata\Builder;

use Keboola\DbExtractor\Exception\InvalidArgumentException;
use Keboola\DbExtractor\Exception\InvalidStateException;
use Keboola\DbExtractor\Metadata\Builder\Builder;
use Keboola\DbExtractor\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\Metadata\Builder\TableBuilder;
use Keboola\DbExtractor\Metadata\ValueObject\Column;
use Keboola\DbExtractor\Metadata\ValueObject\Table;
use PHPUnit\Framework\Assert;

class TableBuilderTest extends BaseBuilderTest
{
    public function createBuilder(array $additionalRequiredProperties = []): TableBuilder
    {
        return TableBuilder::create($additionalRequiredProperties);
    }

    public function getAllProperties(): array
    {
        return [
            'name',
            'description',
            'schema',
            'catalog',
            'type',
            'rowCount',
        ];
    }

    public function getAlwaysRequiredProperties(): array
    {
        return TableBuilder::ALWAYS_REQUIRED_PROPERTIES;
    }

    public function getOptionalRequiredProperties(): array
    {
        return TableBuilder::OPTIONAL_REQUIRED_PROPERTIES;
    }

    public function getNullableProperties(): array
    {
        return [
            'description' => self::NULL_MEANS_NOT_SET,
            'schema' => self::NULL_MEANS_NOT_SET,
            'catalog' => self::NULL_MEANS_NOT_SET,
            'rowCount' => self::NULL_MEANS_NOT_SET,
            'columns' => self::NULL_MEANS_NOT_SET,
        ];
    }

    public function getEmptyStringNotAllowedProperties(): array
    {
        return [
            'name',
        ];
    }

    public function getEmptyStringConvertToNullProperties(): array
    {
        return [
            'description',
            'schema',
            'catalog',
        ];
    }

    public function getDefaultValues(): array
    {
        return [];
    }

    public function getSetCallbacks(): array
    {
        return [
            'name' => function (TableBuilder $builder, $v) {
                return $builder->setName($v);
            },
            'description' => function (TableBuilder $builder, $v) {
                return $builder->setDescription($v);
            },
            'schema' => function (TableBuilder $builder, $v) {
                return $builder->setSchema($v);
            },
            'catalog' => function (TableBuilder $builder, $v) {
                return $builder->setCatalog($v);
            },
            'type' => function (TableBuilder $builder, $v) {
                return $builder->setType($v);
            },
            'rowCount' => function (TableBuilder $builder, $v) {
                return $builder->setRowCount($v);
            },
            'columns' => function (TableBuilder $builder, ?array $v) {
                foreach ($v ?? [] as $name) {
                    $builder->addColumn()->setName($name)->setType('INTEGER');
                }
                return $builder;
            },
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
                return  $column->getColumns()->getAll();
            },
        ];
    }

    public function getValidInputs(): array
    {
        return [
            [
                'name' => 'My Table',
                'description' => 'Some description.',
                'schema' => 'My Schema',
                'catalog' => 'My Catalog',
                'type' => 'table',
                'rowCount' => 25,
            ],
        ];
    }

    public function testSanitizedName(): void
    {
        $name = '#$%_čřž Table 123';
        $properties = $this->getValidInputs()[0];
        $properties['name'] = $name;

        /** @var Table $valueObject */
        $valueObject = $this->buildFromArray($properties);
        Assert::assertSame($name, $valueObject->getName());
        Assert::assertSame('crz_Table_123', $valueObject->getSanitizedName());
    }

    public function testNoColumn(): void
    {
        $builder = TableBuilder::create();
        $builder->setName('Table name');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Table "Table name" must have at least one column.');
        $builder->build();
    }

    public function testColumnsNotExpectedNoColumn(): void
    {
        $builder = TableBuilder::create();
        $builder->setName('Table name');
        $builder->setColumnsNotExpected();

        $table = $builder->build();
        Assert::assertFalse($table->hasColumns());
    }

    public function testAddColumn(): void
    {
        $builder = TableBuilder::create();
        $builder->setName('Table name');
        $columnBuilder = $builder->addColumn();
        Assert::assertTrue($columnBuilder instanceof ColumnBuilder);

        // Set some values
        $columnBuilder
            ->setOrdinalPosition(123)
            ->setName('Col 1')
            ->setType('integer');

        // Assert name
        $table = $builder->build();
        Assert::assertSame('Table name', $table->getName());
        Assert::assertSame('Table_name', $table->getSanitizedName());

        // Assert columns
        $columns = $table->getColumns()->getAll();
        Assert::assertCount(1, $columns);
        $column = $columns[0];
        Assert::assertTrue($column instanceof Column);
        Assert::assertSame(123, $column->getOrdinalPosition());
        Assert::assertSame('Col 1', $column->getName());
        Assert::assertSame('Col_1', $column->getSanitizedName());
        Assert::assertSame('integer', $column->getType());
    }

    public function testColumnsNotExpectedAddColumn(): void
    {
        $builder = TableBuilder::create();
        $builder->setName('Table name');
        $builder->setColumnsNotExpected();

        $this->expectException(InvalidStateException::class);
        $this->expectExceptionMessage('Columns are not expected.');
        $builder->addColumn();
    }

    protected function modifyBuilder(Builder $builder): void
    {
        parent::modifyBuilder($builder);

        // At least one column is required
        /** @var TableBuilder $builder */
        $builder
            ->addColumn()
            ->setOrdinalPosition(123)
            ->setName('Col 1')
            ->setType('integer');
    }
}
