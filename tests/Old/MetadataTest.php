<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Tests\Old;

use Keboola\DbExtractorCommon\BaseExtractor;
use Keboola\DbExtractorCommon\DatabaseMetadata\Column;
use Keboola\DbExtractorCommon\DatabaseMetadata\Table;
use PHPUnit\Framework\TestCase;

class MetadataTest extends TestCase
{
    public function testTableMetadata(): void
    {
        $sourceData = new Table(
            'simple',
            'testdb',
            'BASE TABLE'
        );
        $expectedOutput = [
            [
                "key" => "KBC.name",
                "value" =>"simple",
            ],[
                "key" => "KBC.sanitizedName",
                "value" => "simple",
            ],[
                "key" => "KBC.schema",
                "value" => "testdb",
            ],[
                "key" => "KBC.type",
                "value" => "BASE TABLE",
            ],
        ];
        $outputMetadata = BaseExtractor::getTableLevelMetadata($sourceData);
        $this->assertEquals($expectedOutput, $outputMetadata);
    }

    public function testColumnMetadata(): void
    {
        $testColumn = new Column(
            '_weird-I-d',
            'varchar',
            true,
            '155',
            false,
            'abc',
            1
        );
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
                    'value' => 1,
                ),
        );

        $outputMetadata = BaseExtractor::getColumnMetadata($testColumn);
        $this->assertEquals($expectedOutput, $outputMetadata);
    }
}
