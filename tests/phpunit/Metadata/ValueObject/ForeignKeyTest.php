<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Metadata\ValueObject;

use Keboola\DbExtractor\Metadata\ValueObject\ForeignKey;
use Keboola\DbExtractor\ValueObject;
use Keboola\DbExtractor\Tests\ValueObject\BaseValueObjectTest;

class ForeignKeyTest extends BaseValueObjectTest
{
    public function createValueObjectFromArray(array $properties): ValueObject
    {
        $refSchema = $properties['refSchema'];
        $refTable = $properties['refTable'];
        $refColumn = $properties['refColumn'];
        return new ForeignKey(
            $refSchema,
            $refTable,
            $refColumn
        );
    }

    public function getAllProperties(): array
    {
        return [
            'refSchema',
            'refTable',
            'refColumn',
        ];
    }

    public function getNullableProperties(): array
    {
        return [
            // These properties CAN be set to null in Builder
            // NONE
            // These properties CANNOT be set to null in Builder
            // ... null value in constructor means "not set"
            'refSchema' => self::NULL_MEANS_NOT_SET,
        ];
    }

    public function getEmptyStringNotAllowedProperties(): array
    {
        return [
            'refSchema',
            'refTable',
            'refColumn',
        ];
    }

    public function getHasCallbacks(): array
    {
        return [
            'refSchema' => function (ForeignKey $column) {
                return $column->hasRefSchema();
            },
        ];
    }

    public function getGetCallbacks(): array
    {
        return [
            'refSchema' => function (ForeignKey $column) {
                return $column->getRefSchema();
            },
            'refTable' => function (ForeignKey $column) {
                return $column->getRefTable();
            },
            'refColumn' => function (ForeignKey $column) {
                return $column->getRefColumn();
            },
        ];
    }

    public function getValidInputs(): array
    {
        return [
            [
                'refSchema' => 'schema',
                'refTable' => 'table',
                'refColumn' => 'column',
            ],
        ];
    }
}
