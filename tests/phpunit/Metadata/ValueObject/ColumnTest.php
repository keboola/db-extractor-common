<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Metadata\ValueObject;

use Keboola\DbExtractor\Exception\InvalidArgumentException;
use Keboola\DbExtractor\Exception\PropertyNotSetException;
use Keboola\DbExtractor\Metadata\Builder\ForeignKeyBuilder;
use Keboola\DbExtractor\Metadata\ValueObject\Column;
use Keboola\DbExtractor\ValueObject;
use Keboola\DbExtractor\Tests\ValueObject\BaseValueObjectTest;
use PHPUnit\Framework\Assert;

class ColumnTest extends BaseValueObjectTest
{
    public function createValueObjectFromArray(array $properties): ValueObject
    {
        $name = $properties['name'];
        $sanitizedName = $properties['sanitizedName'];
        $description = $properties['description'];
        $ordinalPosition = $properties['ordinalPosition'];
        $type = $properties['type'];
        $nullable = $properties['nullable'];
        $length = $properties['length'];
        $primaryKey = $properties['primaryKey'];
        $uniqueKey = $properties['uniqueKey'];
        $foreignKey = $properties['foreignKey'];
        $constraints = $properties['constraints'];

        if (array_key_exists('autoIncrement', $properties)) {
            $hasAutoIncrement = $properties['hasAutoIncrement'] ?? true;
            $autoIncrement= $properties['autoIncrement'];
        } else {
            $hasAutoIncrement = $properties['hasAutoIncrement'] ?? false;
            $autoIncrement= null;
        }

        if (array_key_exists('default', $properties)) {
            $hasDefault = $properties['hasDefault'] ?? true;
            ;
            $default = $properties['default'];
        } else {
            $hasDefault = $properties['hasDefault'] ?? false;
            ;
            $default = null;
        }

        return new Column(
            $name,
            $sanitizedName,
            $description,
            $ordinalPosition,
            $type,
            $nullable,
            $length,
            $primaryKey,
            $uniqueKey,
            $hasAutoIncrement,
            $autoIncrement,
            $hasDefault,
            $default,
            $foreignKey,
            $constraints
        );
    }

    public function getAllProperties(): array
    {
        return [
            'name',
            'sanitizedName',
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

    public function getNullableProperties(): array
    {
        return [
            // These properties CAN be set to null in Builder
            'description' => self::NULL_MEANS_NOT_SET,
            'length' => self::NULL_MEANS_NOT_SET,
            'default' => self::NULL_IS_REGULAR_DEFAULT_NOT_SET,
            // These properties CANNOT be set to null in Builder
            // ... null value in constructor means "not set"
            'ordinalPosition' => self::NULL_MEANS_NOT_SET,
            'nullable' => self::NULL_MEANS_NOT_SET,
            'autoIncrement' => self::NULL_MEANS_NOT_SET,
            'foreignKey' => self::NULL_MEANS_NOT_SET,
        ];
    }

    public function getEmptyStringNotAllowedProperties(): array
    {
        return [
            'name',
            'sanitizedName',
            'description',
            'type',
            'length',
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
            'sanitizedName' => function (Column $column) {
                return $column->getSanitizedName();
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
                return $column->getForeignKey();
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
                'sanitizedName' => 'id',
                'description' => 'Id',
                'ordinalPosition' => 0,
                'type' => 'INTEGER',
                'nullable' => false,
                'length' => null,
                'primaryKey' => true,
                'uniqueKey' => false,
                'autoIncrement' => 25,
                'foreignKey' => null,
                'constraints' => [],
            ],
            [
                'name' => 'Col Name',
                'sanitizedName' => 'col_name',
                'description' => 'Some description',
                'ordinalPosition' => 1,
                'type' => 'VARCHAR',
                'nullable' => true,
                'length' => '255',
                'primaryKey' => false,
                'uniqueKey' => true,
                'autoIncrement' => null,
                'default' => 'default text',
                'foreignKey' => ForeignKeyBuilder::create()
                    ->setRefTable('table')->setRefColumn('col')->build(),
                'constraints' => ['abc', 'def'],
            ],
        ];
    }

    public function testAutoIncrement(): void
    {
        $properties = $this->getValidInputs()[0];
        $properties['hasAutoIncrement'] = true;
        $properties['autoIncrement'] = 25;

        /** @var Column $column */
        $column = $this->createValueObjectFromArray($properties);
        Assert::assertSame(true, $column->isAutoIncrement());
        Assert::assertSame(25, $column->getAutoIncrementValue());
    }

    public function testAutoIncrementValueNotSet(): void
    {
        $properties = $this->getValidInputs()[0];
        $properties['hasAutoIncrement'] = true;
        $properties['autoIncrement'] = null;

        /** @var Column $column */
        $column = $this->createValueObjectFromArray($properties);
        Assert::assertSame(true, $column->isAutoIncrement());

        $this->expectException(PropertyNotSetException::class);
        $column->getAutoIncrementValue();
    }

    public function testNotAutoIncrement(): void
    {
        $properties = $this->getValidInputs()[0];
        $properties['hasAutoIncrement'] = false;
        $properties['autoIncrement'] = null;

        /** @var Column $column */
        $column = $this->createValueObjectFromArray($properties);
        Assert::assertSame(false, $column->isAutoIncrement());

        $this->expectException(PropertyNotSetException::class);
        $column->getAutoIncrementValue();
    }

    public function testNotAutoIncrementButValueSet(): void
    {
        $properties = $this->getValidInputs()[0];
        $properties['hasAutoIncrement'] = false;
        $properties['autoIncrement'] = 123;

        $this->expectException(InvalidArgumentException::class);
        $this->createValueObjectFromArray($properties);
    }

    public function testDefault(): void
    {
        $properties = $this->getValidInputs()[0];
        $properties['hasDefault'] = true;
        $properties['default'] = 'Some text';

        /** @var Column $column */
        $column = $this->createValueObjectFromArray($properties);
        Assert::assertSame(true, $column->hasDefault());
        Assert::assertSame('Some text', $column->getDefault());
    }

    public function testDefaultNull(): void
    {
        $properties = $this->getValidInputs()[0];
        $properties['hasDefault'] = true;
        $properties['default'] = null;

        /** @var Column $column */
        $column = $this->createValueObjectFromArray($properties);
        Assert::assertSame(true, $column->hasDefault());
        Assert::assertSame(null, $column->getDefault());
    }

    public function testNotDefault(): void
    {
        $properties = $this->getValidInputs()[0];
        $properties['hasDefault'] = false;
        $properties['default'] = null;

        /** @var Column $column */
        $column = $this->createValueObjectFromArray($properties);
        Assert::assertSame(false, $column->hasDefault());

        $this->expectException(PropertyNotSetException::class);
        $column->getDefault();
    }

    public function testNotDefaultButValueSet(): void
    {
        $properties = $this->getValidInputs()[0];
        $properties['hasDefault'] = false;
        $properties['default'] = 'Some text';

        $this->expectException(InvalidArgumentException::class);
        $this->createValueObjectFromArray($properties);
    }
}
