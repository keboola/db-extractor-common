<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Adapter\Metadata\MetadataProvider;
use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractor\Adapter\ValueObject\QueryMetadata;
use Keboola\DbExtractor\Manifest\DefaultManifestGenerator;
use Keboola\DbExtractor\Manifest\ManifestGenerator;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\ColumnBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Builder\TableBuilder;
use Keboola\DbExtractor\TableResultFormat\Metadata\Manifest\DefaultManifestSerializer;
use Keboola\DbExtractor\TableResultFormat\Metadata\ValueObject\ColumnCollection;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractorConfig\Configuration\ValueObject\InputTable;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class DefaultManifestGeneratorTest extends TestCase
{
    public function testCsvHasHeader(): void
    {
        $exportConfig = $this->createExportConfig();
        $exportResult = $this->createExportResult();

        $exportResult->method('hasCsvHeader')->willReturn(true);

        $manifestGenerator = $this->createManifestGenerator();
        $manifestData = $manifestGenerator->generate($exportConfig, $exportResult);
        Assert::assertSame([
            'destination' => 'output-table',
            'incremental' => false,
            'primary_key' => ['pk1', 'pk2'],
        ], $manifestData);
    }

    public function testCsvWithoutHeaderTableQuery(): void
    {
        $exportConfig = $this->createExportConfig();
        $exportResult = $this->createExportResult();

        $exportConfig->method('hasQuery')->willReturn(false);
        $exportConfig->method('hasColumns')->willReturn(true);
        $exportConfig->method('getColumns')->willReturn(['pk1', 'pk2', 'name', 'age']);
        $exportConfig->method('getTable')->willReturn(new InputTable('OutputTable', 'Schema'));
        $exportResult->method('hasCsvHeader')->willReturn(false);

        $manifestGenerator = $this->createManifestGenerator();
        $manifestData = $manifestGenerator->generate($exportConfig, $exportResult);

        unset($manifestData['column_metadata']); // generated by DefaultManifestSerializer, tested in lib
        Assert::assertSame([
            'destination' => 'output-table',
            'incremental' => false,
            'primary_key' => ['pk1', 'pk2'],
            'metadata' => [
                [
                    'key' => 'KBC.name',
                    'value' => 'OutputTable',
                ],
                [
                    'key' => 'KBC.sanitizedName',
                    'value' => 'OutputTable',
                ],
                [
                    'key' => 'KBC.schema',
                    'value' => 'Schema',
                ],
            ],
            'columns' => ['pk1', 'pk2', 'name', 'age'],
        ], $manifestData);
    }

    public function testCsvWithoutHeaderCustomQuery(): void
    {
        $exportConfig = $this->createExportConfig();
        $exportResult = $this->createExportResult();
        $queryMetadata = $this
            ->getMockBuilder(QueryMetadata::class)
            ->disableAutoReturnValueGeneration()
            ->getMock();

        $columnsRaw = ['pk1' => 'integer', 'pk2' => 'integer', 'generated_col' => 'string'];
        $columnsMetadata = [];
        foreach ($columnsRaw as $name => $type) {
            $builder = ColumnBuilder::create();
            $builder->setName($name);
            $builder->setType($type);
            $columnsMetadata[] = $builder->build();
        }

        $exportConfig->method('hasQuery')->willReturn(true);
        $exportConfig->method('getTable')->willReturn(new InputTable('OutputTable', 'Schema'));
        $exportResult->method('hasCsvHeader')->willReturn(false);
        $exportResult->method('getQueryMetadata')->willReturn($queryMetadata);
        $queryMetadata->method('getColumns')->willReturn(new ColumnCollection($columnsMetadata));

        $manifestGenerator = $this->createManifestGenerator();
        $manifestData = $manifestGenerator->generate($exportConfig, $exportResult);

        Assert::assertSame([
            'destination' => 'output-table',
            'incremental' => false,
            'primary_key' => ['pk1', 'pk2'],
            'columns' => ['pk1', 'pk2', 'generated_col'],
        ], $manifestData);
    }

    /**
     * @psalm-return MockObject&ExportConfig
     * @return MockObject|ExportConfig
     */
    protected function createExportConfig(): MockObject
    {
        $exportConfig = $this
            ->getMockBuilder(ExportConfig::class)
            ->disableOriginalConstructor()
            ->disableAutoReturnValueGeneration()
            ->getMock();

        $exportConfig->method('getOutputTable')->willReturn('output-table');
        $exportConfig->method('isIncrementalLoading')->willReturn(false);
        $exportConfig->method('hasPrimaryKey')->willReturn(true);
        $exportConfig->method('getPrimaryKey')->willReturn(['pk1', 'pk2']);

        return $exportConfig;
    }

    /**
     * @psalm-return MockObject&ExportResult
     * @return MockObject|ExportResult
     */
    protected function createExportResult(): MockObject
    {
        return $this
            ->getMockBuilder(ExportResult::class)
            ->disableOriginalConstructor()
            ->disableAutoReturnValueGeneration()
            ->getMock();
    }

    protected function createManifestGenerator(): ManifestGenerator
    {
        $metadataProvider = $this
            ->getMockBuilder(MetadataProvider::class)
            ->disableAutoReturnValueGeneration()
            ->getMock();

        $tableBuilder = TableBuilder::create();
        $tableBuilder
            ->setName('OutputTable')
            ->setSchema('Schema');
        $tableBuilder
            ->addColumn()
            ->setName('pk1')
            ->setType('INTEGER');
        $tableBuilder
            ->addColumn()
            ->setName('pk2')
            ->setType('INTEGER');
        $tableBuilder
            ->addColumn()
            ->setName('date')
            ->setType('DATE');
        $tableBuilder
            ->addColumn()
            ->setName('name')
            ->setType('VARCHAR')
            ->setLength('255');
        $tableBuilder
            ->addColumn()
            ->setName('age')
            ->setType('INTEGER');

        $metadataProvider->method('getTable')->willReturn($tableBuilder->build());

        $manifestSerializer = new DefaultManifestSerializer();
        return new DefaultManifestGenerator($metadataProvider, $manifestSerializer);
    }
}
