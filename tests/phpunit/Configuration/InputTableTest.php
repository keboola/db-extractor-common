<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Configuration;

use Keboola\DbExtractor\Configuration\InputTable;
use Keboola\DbExtractor\ValueObject;
use Keboola\DbExtractor\Tests\ValueObject\BaseValueObjectTest;
use PHPUnit\Framework\Assert;

class InputTableTest extends BaseValueObjectTest
{
    public function createValueObjectFromArray(array $properties): ValueObject
    {
        $name = $properties['name'];
        $schema = $properties['schema'];
        return new InputTable(
            $name,
            $schema,
        );
    }

    public function getAllProperties(): array
    {
        return [
            'name',
            'schema',
        ];
    }

    public function getNullableProperties(): array
    {
        return [];
    }

    public function getEmptyStringNotAllowedProperties(): array
    {
        return [
            'name',
            'schema',
        ];
    }

    public function getHasCallbacks(): array
    {
        return [];
    }

    public function getGetCallbacks(): array
    {
        return [
            'name' => function (InputTable $table) {
                return $table->getName();
            },
            'schema' => function (InputTable $table) {
                return $table->getSchema();
            },
        ];
    }

    public function getValidInputs(): array
    {
        return [
            [
                'name' => 'table1',
                'schema' => 'schema1',
            ],
        ];
    }

    public function testFromArray(): void
    {
        $table = InputTable::fromArray([
            'table' => [
                'tableName' => 'table1',
                'schema' => 'schema1',
            ],
        ]);

        Assert::assertSame('table1', $table->getName());
        Assert::assertSame('schema1', $table->getSchema());
    }
}
