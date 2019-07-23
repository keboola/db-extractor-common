<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Extractor\Extractor;
use PHPUnit\Framework\TestCase;

class MetadataTest extends TestCase
{
    public function testTableMetadata(): void
    {
        $sourceData = [
            'name' => 'simple',
            'sanitizedName' => 'simple',
            'schema' => 'testdb',
            'type' => 'BASE TABLE',
            'rowCount' => '2',
        ];
        $expectedOutput = [
            [
                'key' => 'KBC.name',
                'value' =>'simple',
            ],[
                'key' => 'KBC.sanitizedName',
                'value' => 'simple',
            ],[
                'key' => 'KBC.schema',
                'value' => 'testdb',
            ],[
                'key' => 'KBC.type',
                'value' => 'BASE TABLE',
            ],[
                'key' => 'KBC.rowCount',
                'value' => '2',
            ],
        ];
        $outputMetadata = Extractor::getTableLevelMetadata($sourceData);
        $this->assertEquals($expectedOutput, $outputMetadata);
    }

    public function testColumnMetadata(): void
    {
        $testColumn = [
            'name' => '_weird-I-d',
            'sanitizedName' => 'weird_I_d',
            'type' => 'varchar',
            'primaryKey' => true,
            'length' => '155',
            'nullable' => false,
            'default' => 'abc',
            'ordinalPosition' => '1',
        ];
        $expectedOutput = array (
            0 =>
                array (
                    'key' => 'KBC.datatype.type',
                    'value' => 'varchar',
                ),
            1 =>
                array (
                    'key' => 'KBC.datatype.nullable',
                    'value' => false,
                ),
            2 =>
                array (
                    'key' => 'KBC.datatype.basetype',
                    'value' => 'STRING',
                ),
            3 =>
                array (
                    'key' => 'KBC.datatype.length',
                    'value' => '155',
                ),
            4 =>
                array (
                    'key' => 'KBC.datatype.default',
                    'value' => 'abc',
                ),
            5 =>
                array (
                    'key' => 'KBC.sourceName',
                    'value' => '_weird-I-d',
                ),
            6 =>
                array (
                    'key' => 'KBC.sanitizedName',
                    'value' => 'weird_I_d',
                ),
            7 =>
                array (
                    'key' => 'KBC.primaryKey',
                    'value' => true,
                ),
            8 =>
                array (
                    'key' => 'KBC.ordinalPosition',
                    'value' => '1',
                ),
        );

        $outputMetadata = Extractor::getColumnMetadata($testColumn);
        $this->assertEquals($expectedOutput, $outputMetadata);
    }
}
