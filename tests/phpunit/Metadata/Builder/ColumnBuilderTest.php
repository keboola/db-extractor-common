<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Metadata\Builder;

use Keboola\DbExtractor\Exception\InvalidStateException;
use Keboola\DbExtractor\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\Metadata\ValueObject\Column;
use PHPUnit\Framework\Assert;

class ColumnBuilderTest extends BaseBuilderTest
{
    public function createBuilder(array $additionalRequiredProperties = []): ColumnBuilder
    {
        return ColumnBuilder::create($additionalRequiredProperties);
    }

    public function getAllProperties(): array
    {
        return [
            'name',
            'description',
            'ordinalPosition',
            'type',
            'nullable',
            'length',
            'primaryKey',
            'uniqueKey',
            'autoIncrement',
            'default',
            'foreignKey',
            'constraints',
        ];
    }

    public function getAlwaysRequiredProperties(): array
    {
        return ColumnBuilder::ALWAYS_REQUIRED_PROPERTIES;
    }

    public function getOptionalRequiredProperties(): array
    {
        return ColumnBuilder::OPTIONAL_REQUIRED_PROPERTIES;
    }

    public function getNullableProperties(): array
    {
        return [
            'description' => self::NULL_MEANS_NOT_SET,
            'length' => self::NULL_MEANS_NOT_SET,
            'default' => self::NULL_IS_REGULAR_DEFAULT_NOT_SET,
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
            'length',
        ];
    }

    public function getDefaultValues(): array
    {
        return [
            'primaryKey' => false,
            'uniqueKey' => false,
            'constraints' => [],
        ];
    }

    public function getSetCallbacks(): array
    {
        return [
            'name' => function (ColumnBuilder $builder, $v) {
                return $builder->setName($v);
            },
            'description' => function (ColumnBuilder $builder, $v) {
                return $builder->setDescription($v);
            },
            'ordinalPosition' => function (ColumnBuilder $builder, $v) {
                return $builder->setOrdinalPosition($v);
            },
            'type' => function (ColumnBuilder $builder, $v) {
                return $builder->setType($v);
            },
            'nullable' => function (ColumnBuilder $builder, $v) {
                return $builder->setNullable($v);
            },
            'length' => function (ColumnBuilder $builder, $v) {
                return $builder->setLength($v);
            },
            'primaryKey' => function (ColumnBuilder $builder, $v) {
                return $builder->setPrimaryKey($v);
            },
            'uniqueKey' => function (ColumnBuilder $builder, $v) {
                return $builder->setUniqueKey($v);
            },
            'autoIncrement' => function (ColumnBuilder $builder, $v) {
                return $builder->setAutoIncrementValue($v);
            },
            'default' => function (ColumnBuilder $builder, $v) {
                return $builder->setDefault($v);
            },
            'foreignKey' => function (ColumnBuilder $builder, string $name) {
                $builder
                    ->addForeignKey()
                    ->setRefTable($name)
                    ->setRefColumn('refCol');
                return $builder;
            },
            'constraints' => function (ColumnBuilder $builder, array $v) {
                foreach ($v as $constraint) {
                    $builder->addConstraint($constraint);
                }
                return $builder;
            },
        ];
    }

    public function getHasCallbacks(): array
    {
        return [
            'description' => function (Column $column) {
                return  $column->hasDescription();
            },
            'ordinalPosition' => function (Column $column) {
                return  $column->hasOrdinalPosition();
            },
            'nullable' => function (Column $column) {
                return  $column->hasNullable();
            },
            'length' => function (Column $column) {
                return  $column->hasLength();
            },
        ];
    }

    public function getGetCallbacks(): array
    {
        return [
            'name' => function (Column $column) {
                return $column->getName();
            },
            'description' => function (Column $column) {
                return $column->getDescription();
            },
            'ordinalPosition' => function (Column $column) {
                return $column->getOrdinalPosition();
            },
            'type' => function (Column $column) {
                return $column->getType();
            },
            'nullable' => function (Column $column) {
                return $column->isNullable();
            },
            'length' => function (Column $column) {
                return $column->getLength();
            },
            'primaryKey' => function (Column $column) {
                return $column->isPrimaryKey();
            },
            'uniqueKey' => function (Column $column) {
                return $column->isUniqueKey();
            },
            'autoIncrement' => function (Column $column) {
                return $column->getAutoIncrementValue();
            },
            'default' => function (Column $column) {
                return $column->getDefault();
            },
            'foreignKey' => function (Column $column) {
                return $column->getForeignKey()->getRefTable();
            },
            'constraints' => function (Column $column) {
                return $column->getConstraints();
            },
        ];
    }

    public function getValidInputs(): array
    {
        return [
            [
                'name' => 'id',
                'description' => 'Id',
                'ordinalPosition' => 0,
                'type' => 'INTEGER',
                'nullable' => false,
                'length' => null,
                'primaryKey' => true,
                'uniqueKey' => false,
                'autoIncrement' => 25,
            ],
            [
                'name' => 'Col Name',
                'description' => 'Some description',
                'ordinalPosition' => 1,
                'type' => 'VARCHAR',
                'nullable' => true,
                'length' => '255',
                'primaryKey' => false,
                'uniqueKey' => true,
                'default' => 'Some text',
                'foreignKey' => 'fk1',
                'constraints' => ['abc', 'def'],
            ],
        ];
    }

    public function testSanitizedName(): void
    {
        $name = '#$%_čřž Column 123';
        $properties = $this->getValidInputs()[0];
        $properties['name'] = $name;

        /** @var Column $valueObject */
        $valueObject = $this->buildFromArray($properties);
        Assert::assertSame($name, $valueObject->getName());
        Assert::assertSame('crz_Column_123', $valueObject->getSanitizedName());
    }

    public function testForeignKeyAlreadySet(): void
    {
        $builder = $this->createBuilder();

        $builder
            ->addForeignKey()
            ->setRefTable('table1')
            ->setRefColumn('col1');

        $this->expectException(InvalidStateException::class);
        $this->expectExceptionMessage('Foreign key is already set.');
        $builder->addForeignKey();
    }
}
