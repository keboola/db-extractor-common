<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Component\JsonHelper;
use Keboola\Csv\CsvReader;
use Keboola\DbExtractor\Test\ExtractorTest;
use PDO;
use PHPUnit\Framework\Assert;
use Psr\Log\Test\TestLogger;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;

class CommonExtractorLegacyFormatTest extends ExtractorTest
{
    use TestDataTrait;

    public const DRIVER = 'common';

    protected PDO $db;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanStateFiles();
        $this->initDatabase();
        putenv('KBC_DATA_TYPE_SUPPORT=none');
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->closeSshTunnels();
    }

    public function testRunSimple(): void
    {
        $this->cleanOutputDirectory();
        $logger = new TestLogger();
        $this->getApp(
            $this->getConfig(self::DRIVER),
            $logger,
        )->execute();

        Assert::assertTrue($logger->hasInfo('Exporting "escaping" to "in.c-main.escaping".'));
        Assert::assertTrue($logger->hasInfo('Exporting "simple" to "in.c-main.simple".'));

        $filename = $this->dataDir . '/out/tables/in.c-main.simple.csv.manifest';
        $manifest = json_decode(
            (string) file_get_contents($filename),
            true,
        );
        Assert::assertEquals(['weird_I_d', 'SaoPaulo'], $manifest['columns']);
        Assert::assertEquals(['weird_I_d'], $manifest['primary_key']);
        Assert::assertTrue($logger->hasInfoThatContains('Exported "2" rows to "in.c-main.simple".'));
    }

    public function testRunUserInitQueries(): void
    {
        $this->cleanOutputDirectory();
        $logger = new TestLogger();
        $config = $this->getConfigRow(self::DRIVER);
        $config['parameters']['db']['initQueries'] = [
            'TRUNCATE TABLE `simple`',
        ];
        $this->getApp($config, $logger)->execute();
        Assert::assertEquals(
            '',
            file_get_contents($this->dataDir . '/out/tables/in.c-main.simple.csv'),
        );
        $filename = $this->dataDir . '/out/tables/in.c-main.simple.csv.manifest';
        $manifest = json_decode(
            (string) file_get_contents($filename),
            true,
        );
        Assert::assertEquals(['weird_I_d', 'SaoPaulo'], $manifest['columns']);
        Assert::assertEquals(['weird_I_d'], $manifest['primary_key']);
        Assert::assertTrue($logger->hasInfoThatContains('Running query "TRUNCATE TABLE `simple`".'));
        Assert::assertTrue($logger->hasWarningThatContains('Exported "0" rows to "in.c-main.simple".'));
    }

    public function testRunNoPrimaryKey(): void
    {
        $this->cleanOutputDirectory();

        $config = $this->getConfigRow(self::DRIVER);
        $config['parameters']['primaryKey'] = [];

        $logger = new TestLogger();

        $this->getApp($config, $logger)->execute();
        Assert::assertTrue($logger->hasInfo('Exporting "escaping" to "in.c-main.simple".'));
        $filename = $this->dataDir . '/out/tables/in.c-main.simple.csv.manifest';
        $manifest = json_decode(
            (string) file_get_contents($filename),
            true,
        );
        Assert::assertEquals(['weird_I_d', 'SaoPaulo'], $manifest['columns']);
        Assert::assertArrayNotHasKey('primary_key', $manifest);
    }

    public function testRunPrimaryKeyDefinedOnlyInConfig(): void
    {
        $this->cleanOutputDirectory();

        $config = $this->getConfigRow(self::DRIVER);
        $config['parameters']['primaryKey'] = ['SãoPaulo'];

        $logger = new TestLogger();

        $this->getApp($config, $logger)->execute();

        Assert::assertTrue($logger->hasInfo('Exporting "escaping" to "in.c-main.simple".'));

        $filename = $this->dataDir . '/out/tables/in.c-main.simple.csv.manifest';
        $manifest = json_decode(
            (string) file_get_contents($filename),
            true,
        );
        Assert::assertEquals(['weird_I_d', 'SaoPaulo'], $manifest['columns']);
        Assert::assertEquals(['SaoPaulo'], $manifest['primary_key']);
    }

    public function testRunJsonConfig(): void
    {
        $this->cleanOutputDirectory();

        $logger = new TestLogger();

        $this->getApp($this->getConfig(self::DRIVER), $logger)->execute();
        Assert::assertTrue($logger->hasInfo('Exporting "escaping" to "in.c-main.escaping".'));
        Assert::assertTrue($logger->hasInfo('Exporting "simple" to "in.c-main.simple".'));

        $filename = $this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest';
        $manifest = json_decode(
            (string) file_get_contents($filename),
            true,
        );

        Assert::assertArrayNotHasKey('columns', $manifest);
        Assert::assertArrayNotHasKey('primary_key', $manifest);

        $filename = $this->dataDir . '/out/tables/in.c-main.simple.csv.manifest';
        $manifest = json_decode(
            (string) file_get_contents($filename),
            true,
        );
        Assert::assertEquals(['weird_I_d', 'SaoPaulo'], $manifest['columns']);
        Assert::assertEquals(['weird_I_d'], $manifest['primary_key']);
    }

    public function testRunConfigRow(): void
    {
        $this->cleanOutputDirectory();

        $logger = new TestLogger();

        $this->getApp($this->getConfigRow(self::DRIVER), $logger)->execute();
        Assert::assertTrue($logger->hasInfo('Exporting "escaping" to "in.c-main.simple".'));
        Assert::assertTrue($logger->hasInfo('Exported "2" rows to "in.c-main.simple".'));

        $this->assertExtractedData($this->dataDir . '/simple.csv', 'in.c-main.simple');
        $filename = $this->dataDir . '/out/tables/in.c-main.simple.csv.manifest';
        $manifest = json_decode(
            (string) file_get_contents($filename),
            true,
        );
        Assert::assertEquals(['weird_I_d', 'SaoPaulo'], $manifest['columns']);
        Assert::assertEquals(['weird_I_d'], $manifest['primary_key']);
    }

    public function testRunEmptyQuery(): void
    {
        $this->cleanOutputDirectory();
        $outputCsvFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest';

        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['tables'][0]['query'] = 'SELECT * FROM escaping WHERE col1 = \'123\'';

        $logger = new TestLogger();

        $this->getApp($config, $logger)->execute();

        Assert::assertFileExists($outputCsvFile);
        Assert::assertFileExists($outputManifestFile);

        // Csv file contains header (because custom query)
        Assert::assertSame("\"col1\",\"col2\"\n", file_get_contents($outputCsvFile));

        // Manifest doesn't contain columns
        $manifest = json_decode((string) file_get_contents($outputManifestFile), true);
        Assert::assertArrayNotHasKey('columns', $manifest);
    }

    public function testMetadataManifest(): void
    {
        $config = $this->getConfig(self::DRIVER);
        unset($config['parameters']['tables'][0]);

        $manifestFile = $this->dataDir . '/out/tables/in.c-main.simple.csv.manifest';

        $logger = new TestLogger();

        $this->getApp($config, $logger)->execute();

        $this->assertExtractedData($this->dataDir . '/simple.csv', 'in.c-main.simple');

        $outputManifest = json_decode(
            (string) file_get_contents($manifestFile),
            true,
        );

        Assert::assertArrayHasKey('destination', $outputManifest);
        Assert::assertArrayHasKey('incremental', $outputManifest);
        Assert::assertArrayHasKey('metadata', $outputManifest);

        $expectedMetadata = [
            'KBC.name' => 'simple',
            'KBC.schema' => 'testdb',
            'KBC.type' => 'BASE TABLE',
            'KBC.sanitizedName' => 'simple',
        ];
        $metadataList = [];
        foreach ($outputManifest['metadata'] as $i => $metadata) {
            Assert::assertArrayHasKey('key', $metadata);
            Assert::assertArrayHasKey('value', $metadata);
            $metadataList[$metadata['key']] = $metadata['value'];
        }

        Assert::assertEquals(2, $metadataList['KBC.rowCount']);
        unset($metadataList['KBC.rowCount']);

        Assert::assertEquals($expectedMetadata, $metadataList);
        Assert::assertArrayHasKey('column_metadata', $outputManifest);
        Assert::assertCount(2, $outputManifest['column_metadata']);
        Assert::assertArrayHasKey('weird_I_d', $outputManifest['column_metadata']);
        Assert::assertArrayHasKey('SaoPaulo', $outputManifest['column_metadata']);

        $expectedColumnMetadata = [
            'weird_I_d' =>
                [
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'STRING',
                    ],
                    [
                        'key' => 'KBC.datatype.default',
                        'value' => 'abc',
                    ],
                    [
                        'key' => 'KBC.sourceName',
                        'value' => '_weird-I-d',
                    ],
                    [
                        'key' => 'KBC.sanitizedName',
                        'value' => 'weird_I_d',
                    ],
                    [
                        'key' => 'KBC.uniqueKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.ordinalPosition',
                        'value' => '1',
                    ],
                    [
                        'key' => 'KBC.constraintName',
                        'value' => 'PRIMARY',
                    ],
                ],
            'SaoPaulo' =>
                [
                    [
                        'key' => 'KBC.datatype.nullable',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.datatype.basetype',
                        'value' => 'STRING',
                    ],
                    [
                        'key' => 'KBC.datatype.default',
                        'value' => 'abc',
                    ],
                    [
                        'key' => 'KBC.sourceName',
                        'value' => 'SãoPaulo',
                    ],
                    [
                        'key' => 'KBC.sanitizedName',
                        'value' => 'SaoPaulo',
                    ],
                    [
                        'key' => 'KBC.uniqueKey',
                        'value' => false,
                    ],
                    [
                        'key' => 'KBC.ordinalPosition',
                        'value' => '2',
                    ],
                ],
        ];

        $this->assertMetadataEquals($expectedColumnMetadata, (array) $outputManifest['column_metadata']);
    }

    private function assertMetadataEquals(array $expected, array $actual): void
    {
        $expectedSorted = $this->sortMetadata($expected);
        $actualSorted = $this->sortMetadata($actual);

        $this->assertEquals($expectedSorted, $actualSorted);
    }

    private function sortMetadata(array $metadata): array
    {
        foreach ($metadata as &$column) {
            usort($column, function ($a, $b) {
                return $a['key'] <=> $b['key'];
            });
        }
        ksort($metadata);

        return $metadata;
    }

    public function testTableColumnsQuery(): void
    {
        $config = $this->getConfig(self::DRIVER);
        unset($config['parameters']['tables'][0]);

        $this->getApp($config)->execute();

        $this->assertExtractedData($this->dataDir . '/simple.csv', 'in.c-main.simple');
        $manifest = json_decode(
            (string) file_get_contents($this->dataDir . '/out/tables/in.c-main.simple.csv.manifest'),
            true,
        );
        Assert::assertEquals(['weird_I_d', 'SaoPaulo'], $manifest['columns']);
        Assert::assertEquals(['weird_I_d'], $manifest['primary_key']);
    }

    public function testIncrementalLoadingEnabledIncrementalFetchingDisabled(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        $config['parameters']['incremental'] = true;
        unset($config['parameters']['incrementalFetchingColumn']);

        $logger = new TestLogger();
        $this->getApp($config, $logger)->execute();

        Assert::assertTrue($logger->hasInfo('Exported "2" rows to "in.c-main.simple".'));

        $stateFile = $this->dataDir . '/out/state.json';

        Assert::assertFileDoesNotExist($stateFile);

        // Check extracted data
        $this->assertExtractedData($this->dataDir . '/simple.csv', 'in.c-main.simple');

        // Check manifest incremental key
        $outputManifest = JsonHelper::readFile(
            $this->dataDir . '/out/tables/in.c-main.simple.csv.manifest',
        );
        Assert::assertTrue($outputManifest['incremental']);
    }

    public function testColumnOrdering(): void
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['columns'] = ['timestamp', 'id', 'name'];
        $config['parameters']['outputTable'] = 'in.c-main.columnsCheck';
        $this->getApp($config)->execute();
        $outputManifestFile = $this->dataDir . '/out/tables/in.c-main.columnscheck.csv.manifest';

        $outputManifest = json_decode(
            (string) file_get_contents($outputManifestFile),
            true,
        );

        // check that the manifest has the correct column ordering
        Assert::assertEquals($config['parameters']['columns'], $outputManifest['columns']);
        // check the data
        $expectedData = iterator_to_array(new CsvReader($this->dataDir . '/columnsOrderCheck.csv'));
        $outputData = iterator_to_array(new CsvReader($this->dataDir . '/out/tables/in.c-main.columnscheck.csv'));
        Assert::assertCount(2, $outputData);
        foreach ($outputData as $rowNum => $line) {
            // assert timestamp
            Assert::assertNotFalse(strtotime($line[0]));
            Assert::assertEquals($line[1], $expectedData[$rowNum][1]);
            Assert::assertEquals($line[2], $expectedData[$rowNum][2]);
        }
    }

    protected function createAutoIncrementAndTimestampTable(): void
    {
        $this->db->exec('DROP TABLE IF EXISTS auto_increment_timestamp');

        $this->db->exec(
            'CREATE TABLE auto_increment_timestamp (
            `id` INT NOT NULL AUTO_INCREMENT,
            `name` VARCHAR(30) NOT NULL DEFAULT \'pam\',
            `number` DECIMAL(25,20) NOT NULL DEFAULT 0.0,
            `timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`)
        )',
        );
        $this->db->exec('INSERT INTO auto_increment_timestamp (`name`) VALUES (\'george\'), (\'henry\')');
    }

    protected function assertExtractedData(
        string $expectedCsvFile,
        string $outputName,
        bool $headerExpected = true,
    ): void {
        $outputCsvFile = $this->dataDir . '/out/tables/' . $outputName . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $outputName . '.csv.manifest';

        Assert::assertFileExists($outputCsvFile);
        Assert::assertFileExists($outputManifestFile);
        Assert::assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
    }


    private function getIncrementalFetchingConfig(): array
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['query']);
        unset($config['parameters']['columns']);
        $config['parameters']['table'] = [
            'tableName' => 'auto_increment_timestamp',
            'schema' => 'testdb',
        ];
        $config['parameters']['incremental'] = true;
        $config['parameters']['name'] = 'auto-increment-timestamp';
        $config['parameters']['outputTable'] = 'in.c-main.auto-increment-timestamp';
        $config['parameters']['primaryKey'] = ['id'];
        $config['parameters']['incrementalFetchingColumn'] = 'id';
        return $config;
    }

    private function cleanOutputDirectory(): void
    {
        $finder = new Finder();
        if (file_exists($this->dataDir . '/out/tables')) {
            $finder->files()->in($this->dataDir . '/out/tables');
            $fs = new Filesystem();
            foreach ($finder as $file) {
                $fs->remove((string) $file);
            }
        }
    }

    private function cleanStateFiles(): void
    {
        foreach (['in', 'out'] as $item) {
            $stateFile = $this->dataDir . '/' . $item . '/state.json';
            if (file_exists($stateFile)) {
                $fs = new Filesystem();
                $fs->remove($stateFile);
            }
        }
    }
}
