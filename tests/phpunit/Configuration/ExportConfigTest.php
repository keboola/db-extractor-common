<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Configuration;

use Keboola\DbExtractor\Configuration\ExportConfig;
use Keboola\DbExtractor\Configuration\IncrementalFetchingConfig;
use Keboola\DbExtractor\Configuration\InputTable;
use Keboola\DbExtractor\Exception\InvalidArgumentException;
use Keboola\DbExtractor\ValueObject;
use Keboola\DbExtractor\Tests\ValueObject\BaseValueObjectTest;
use PHPUnit\Framework\Assert;

class ExportConfigTest extends BaseValueObjectTest
{
    public function createValueObjectFromArray(array $properties): ValueObject
    {
        $query = $properties['query'];
        $table = $properties['table'];
        $incrementalFetchingConfig = $properties['incrementalFetchingConfig'];
        $columns = $properties['columns'];
        $outputTable = $properties['outputTable'];
        $primaryKey = $properties['primaryKey'];
        $maxRetries = $properties['maxRetries'];
        return new ExportConfig(
            $query,
            $table,
            $incrementalFetchingConfig,
            $columns,
            $outputTable,
            $primaryKey,
            $maxRetries,
        );
    }

    public function getAllProperties(): array
    {
        return [
            'query',
            'table',
            'incrementalFetchingConfig',
            'columns',
            'outputTable',
            'primaryKey',
            'maxRetries',
        ];
    }

    public function getNullableProperties(): array
    {
        return [
            'query' => self::NULL_MEANS_NOT_SET,
            'table' => self::NULL_MEANS_NOT_SET,
            'incrementalFetchingConfig' => self::NULL_MEANS_NOT_SET,
            'columns' => self::NULL_MEANS_NOT_SET,
            'primaryKey' => self::NULL_MEANS_NOT_SET,
        ];
    }

    public function getEmptyStringNotAllowedProperties(): array
    {
        return [
            'query',
            'outputTable',
        ];
    }

    public function getHasCallbacks(): array
    {
        return [
            'query' => function (ExportConfig $export) {
                return $export->hasQuery();
            },
            'table' => function (ExportConfig $export) {
                return $export->hasTable();
            },
            'incrementalFetchingConfig' => function (ExportConfig $export) {
                return $export->isIncremental();
            },
            'columns' => function (ExportConfig $export) {
                return $export->hasColumns();
            },
            'primaryKey' => function (ExportConfig $export) {
                return $export->hasPrimaryKey();
            },
        ];
    }

    public function getGetCallbacks(): array
    {
        return [
            'query' => function (ExportConfig $export) {
                return $export->getQuery();
            },
            'table' => function (ExportConfig $export) {
                return $export->getTable();
            },
            'incrementalFetchingConfig' => function (ExportConfig $export) {
                return $export->getIncrementalFetchingConfig();
            },
            'columns' => function (ExportConfig $export) {
                return $export->getColumns();
            },
            'outputTable' => function (ExportConfig $export) {
                return $export->getOutputTable();
            },
            'primaryKey' => function (ExportConfig $export) {
                return $export->getPrimaryKey();
            },
            'maxRetries' => function (ExportConfig $export) {
                return $export->getMaxRetries();
            },
        ];
    }

    public function getValidInputs(): array
    {
        return [
            'minimal' => [
                'query' => null,
                'table' => InputTable::fromArray(['table' => ['tableName' => 'table1', 'schema' => 'schema1']]),
                'incrementalFetchingConfig' => null,
                'columns' => null,
                'outputTable' => 'output',
                'primaryKey' => null,
                'maxRetries' => 3,
            ],
            'full' => [
                'query' => null,
                'table' => InputTable::fromArray(['table' => ['tableName' => 'table1', 'schema' => 'schema1']]),
                'incrementalFetchingConfig' => IncrementalFetchingConfig::fromArray([
                    'incremental' => true,
                    'incrementalFetchingColumn' => 'b',
                    'incrementalFetchingLimit' => 100,
                ]),
                'columns' => ['a', 'b', 'c'],
                'outputTable' => 'output',
                'primaryKey' => ['a', 'c'],
                'maxRetries' => 5,
            ],
            'custom-query' => [
                'query' => 'SELECT * FROM `test`',
                'table' => null,
                'incrementalFetchingConfig' => IncrementalFetchingConfig::fromArray([
                    'incremental' => true,
                    'incrementalFetchingColumn' => 'b',
                    'incrementalFetchingLimit' => 100,
                ]),
                'columns' => null,
                'outputTable' => 'output',
                'primaryKey' => ['a', 'c'],
                'maxRetries' => 5,
            ],
        ];
    }

    public function getValidDataProvider(): iterable
    {
        foreach (parent::getValidDataProvider() as $data) {
            $properties = $data[0];

            // One of query or table must be set, not valid input
            if ($properties['query'] === null && $properties['table'] === null) {
                continue;
            }

            yield $data;
        }
    }

    public function testFromArray(): void
    {
        $export = ExportConfig::fromArray([
            'table' => [
                'tableName' => 'table1',
                'schema' => 'schema1',
            ],
            'incremental' => true,
            'incrementalFetchingColumn' => 'abc',
            'incrementalFetchingLimit' => 100,
            'columns' => ['abc', 'def'],
            'outputTable' => 'output',
            'primaryKey' => ['def'],
            'retries' => 10,
        ]);

        Assert::assertSame('table1', $export->getTable()->getName());
        Assert::assertSame('schema1', $export->getTable()->getSchema());
        Assert::assertSame(true, $export->isIncremental());
        Assert::assertSame('abc', $export->getIncrementalColumn());
        Assert::assertSame('abc', $export->getIncrementalFetchingConfig()->getColumn());
        Assert::assertSame(100, $export->getIncrementalLimit());
        Assert::assertSame(100, $export->getIncrementalFetchingConfig()->getLimit());
        Assert::assertSame(['abc', 'def'], $export->getColumns());
        Assert::assertSame('output', $export->getOutputTable());
        Assert::assertSame(['def'], $export->getPrimaryKey());
        Assert::assertSame(10, $export->getMaxRetries());
    }

    public function testQueryOrTableMustBeSet(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Query or table must be specified.');
        new ExportConfig(
            null,
            null,
            null,
            null,
            'output',
            null,
            5,
        );
    }

    public function testQueryOrTableMustBeSetFromArray(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Key "table" or "query" must be set.');
        ExportConfig::fromArray([
            'outputTable' => 'output',
            'maxRetries' => 5,
        ]);
    }

    public function testDefaultMaxRetries(): void
    {
        $export = ExportConfig::fromArray([
            'query' => 'SELECT * FROM `test`',
            'outputTable' => 'output',
        ]);
        Assert::assertSame(ExportConfig::DEFAULT_MAX_TRIES, $export->getMaxRetries());
    }

    public function testMaxRetriesMinValue(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Max retries must be >= 0.');
        ExportConfig::fromArray([
            'query' => 'SELECT * FROM `test`',
            'outputTable' => 'output',
            'retries' => -1,
        ]);
    }

    public function testPrimaryKeyCannotBeEmptyArray(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Primary key cannot be empty array, null expected.');
        new ExportConfig(
            'SELECT * FROM `test`',
            null,
            null,
            null,
            'output',
            [],
            5,
        );
    }

    public function testColumnsCannotBeEmptyArray(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Columns cannot be empty array, null expected.');
        new ExportConfig(
            'SELECT * FROM `test`',
            null,
            null,
            [],
            'output',
            null,
            5,
        );
    }
}
