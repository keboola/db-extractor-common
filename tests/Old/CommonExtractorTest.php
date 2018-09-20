<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Old;

use Keboola\Csv\CsvReader;
use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Test\DataLoader;
use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;

class CommonExtractorTest extends ExtractorTest
{
    public const DRIVER = 'common';

    /** @var string */
    protected $appName = 'ex-db-common';

    /**
     * @var  \PDO
     */
    private $db;

    public function setUp(): void
    {
        $this->initDatabase();
    }

    private function getApp(array $config, array $state = []): Application
    {
        return parent::getApplication($this->appName, $config, $state);
    }

    private function initDatabase(): void
    {
        $dataLoader = new DataLoader(
            $this->getEnv(self::DRIVER, 'DB_HOST'),
            $this->getEnv(self::DRIVER, 'DB_PORT'),
            $this->getEnv(self::DRIVER, 'DB_DATABASE'),
            $this->getEnv(self::DRIVER, 'DB_USER'),
            $this->getEnv(self::DRIVER, 'DB_PASSWORD')
        );

        $dataLoader->getPdo()->exec(sprintf(
            "DROP DATABASE IF EXISTS `%s`",
            $this->getEnv(self::DRIVER, 'DB_DATABASE')
        ));
        $dataLoader->getPdo()->exec(
            sprintf(
                "
            CREATE DATABASE `%s`
            DEFAULT CHARACTER SET utf8
            DEFAULT COLLATE utf8_general_ci
        ",
                $this->getEnv(self::DRIVER, 'DB_DATABASE')
            )
        );

        $dataLoader->getPdo()->exec("USE " . $this->getEnv(self::DRIVER, 'DB_DATABASE'));

        $dataLoader->getPdo()->exec("SET NAMES utf8;");
        $dataLoader->getPdo()->exec(
            "CREATE TABLE escapingPK (
                                    col1 VARCHAR(155), 
                                    col2 VARCHAR(155), 
                                    PRIMARY KEY (col1, col2))"
        );

        $dataLoader->getPdo()->exec(
            "CREATE TABLE escaping (
                                  col1 VARCHAR(155) NOT NULL DEFAULT 'abc', 
                                  col2 VARCHAR(155) NOT NULL DEFAULT 'abc',
                                  FOREIGN KEY (col1, col2) REFERENCES escapingPK(col1, col2))"
        );

        $dataLoader->getPdo()->exec(
            "CREATE TABLE simple (
                                  `_weird-I-d` VARCHAR(155) NOT NULL DEFAULT 'abc', 
                                  `SãoPaulo` VARCHAR(155) NOT NULL DEFAULT 'abc',
                                  PRIMARY KEY (`_weird-I-d`))"
        );

        $simpleFile = $this->dataDir . '/simple.csv';
        $dataLoader->load($simpleFile, 'simple', 0);
        // let other methods use the db connection
        $this->db = $dataLoader->getPdo();
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

    public function testRunConfigRow(): void
    {
        $this->cleanOutputDirectory();
        $result = ($this->getApp($this->getConfigRow(self::DRIVER)))->run();
        $this->assertEquals('success', $result['status']);
        $this->assertEquals('in.c-main.simple', $result['imported']['outputTable']);
        $this->assertEquals(2, $result['imported']['rows']);
        $this->assertExtractedData($this->dataDir . '/simple.csv', $result['imported']['outputTable']);
        $manifest = json_decode(
            (string) file_get_contents(sprintf(
                "%s/out/tables/%s.csv.manifest",
                $this->dataDir,
                $result['imported']['outputTable']
            )),
            true
        );
        $this->assertEquals(["weird_I_d", 'S_oPaulo'], $manifest['columns']);
        $this->assertEquals(["weird_I_d"], $manifest['primary_key']);
    }

    public function testIncrementalFetchingByTimestamp(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['incrementalFethcingColumn'] = 'timestamp';
        $this->createAutoIncrementAndTimestampTable();

        $result = ($this->getApp($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertNotEmpty($result['state']['lastFetchedRow']);

        sleep(2);
        // the next fetch should be empty
        $emptyResult = ($this->getApp($config, $result['state']))->run();
        $this->assertEquals(0, $emptyResult['imported']['rows']);

        sleep(2);
        //now add a couple rows and run it again.
        $this->db->exec('INSERT INTO auto_increment_timestamp (`name`) VALUES (\'charles\'), (\'william\')');

        $newResult = ($this->getApp($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertGreaterThan(
            $result['state']['lastFetchedRow'],
            $newResult['state']['lastFetchedRow']
        );
    }

    public function testIncrementalFetchingByAutoIncrement(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['incrementalFethcingColumn'] = 'id';
        $this->createAutoIncrementAndTimestampTable();

        $result = ($this->getApp($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(2, $result['state']['lastFetchedRow']);

        sleep(2);
        // the next fetch should be empty
        $emptyResult = ($this->getApp($config, $result['state']))->run();
        $this->assertEquals(0, $emptyResult['imported']['rows']);

        sleep(2);
        //now add a couple rows and run it again.
        $this->db->exec('INSERT INTO auto_increment_timestamp (`name`) VALUES (\'charles\'), (\'william\')');

        $newResult = ($this->getApp($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertEquals(4, $newResult['state']['lastFetchedRow']);
        $this->assertEquals(2, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingLimit(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingLimit'] = 1;
        $this->createAutoIncrementAndTimestampTable();

        $result = ($this->getApp($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 1,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(1, $result['state']['lastFetchedRow']);

        sleep(2);
        // the next fetch should contain the second row
        $result = ($this->getApp($config, $result['state']))->run();
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 1,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(2, $result['state']['lastFetchedRow']);
    }

    public function testIncrementalFetchingDisabled(): void
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = ''; // unset
        $result = ($this->getApp($config))->run();

        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertEmpty($result['state']);
    }

    public function testIncrementalFetchingInvalidColumns(): void
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'fakeCol'; // column does not exist

        try {
            $result = ($this->getApp($config))->run();
            $this->fail('specified autoIncrement column does not exist, should fail.');
        } catch (UserException $e) {
            $this->assertStringStartsWith("Column [fakeCol]", $e->getMessage());
        }

        // column exists but is not auto-increment nor updating timestamp so should fail
        $config['parameters']['incrementalFetchingColumn'] = 'name';
        try {
            $result = ($this->getApp($config))->run();
            $this->fail('specified column is not auto increment nor timestamp, should fail.');
        } catch (UserException $e) {
            $this->assertStringStartsWith("Column [name] specified for incremental fetching", $e->getMessage());
        }
    }

    public function testIncrementalFetchingInvalidConfig(): void
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['query'] = 'SELECT * FROM auto_increment_timestamp';
        unset($config['parameters']['table']);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Invalid configuration for path "parameters": Incremental fetching is not supported for advanced query.'
        );
        $app = $this->getApp($config);
        $app->run();
    }

    public function testColumnOrdering(): void
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['columns'] = ['timestamp', 'id', 'name'];
        $config['parameters']['outputTable'] = 'in.c-main.columnsCheck';
        $result = $this->getApp($config)->run();
        $this->assertEquals('success', $result['status']);
        $outputManifestFile = $this->dataDir . '/out/tables/in.c-main.columnscheck.csv.manifest';

        $outputManifest = json_decode(
            (string) file_get_contents($outputManifestFile),
            true
        );

        // check that the manifest has the correct column ordering
        $this->assertEquals($config['parameters']['columns'], $outputManifest['columns']);
        // check the data
        $expectedData = iterator_to_array(new CsvReader($this->dataDir.'/columnsOrderCheck.csv'));
        $outputData = iterator_to_array(new CsvReader($this->dataDir.'/out/tables/in.c-main.columnscheck.csv'));
        $this->assertCount(2, $outputData);
        foreach ($outputData as $rowNum => $line) {
            // assert timestamp
            $this->assertNotFalse(strtotime($line[0]));
            $this->assertEquals($line[1], $expectedData[$rowNum][1]);
            $this->assertEquals($line[2], $expectedData[$rowNum][2]);
        }
    }

    public function testActionTestConnectionWithoutDeepConfigValidation(): void
    {
        $config = [
            'action' => 'testConnection',
            'parameters' => [
                'db' => $this->getConfigDbNode(self::DRIVER),
                'data_dir' => $this->dataDir,
                'extractor_class' => ucfirst(self::DRIVER),
            ],
        ];

        $result = ($this->getApp($config))->run();
        $this->assertCount(1, $result);
        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testConfigWithNoName(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['name']);
        unset($config['parameters']['table']);
        // we want to test the no results case
        $config['parameters']['query'] = "SELECT 1 LIMIT 0";
        $result = ($this->getApp($config))->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testInvalidConfigsBothTableAndQueryWithNoName(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['name']);

        // we want to test the no results case
        $config['parameters']['query'] = "SELECT 1 LIMIT 0";

        $this->expectException(UserException::class);
        $this->expectExceptionMessageRegExp('(.*Both table and query cannot be set together.*)');

        ($this->getApp($config))->run();
    }

    public function testInvalidConfigsBothIncrFetchAndQueryWithNoName(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['name']);
        unset($config['parameters']['table']);
        $config['parameters']['incrementalFetchingColumn'] = 'abc';

        // we want to test the no results case
        $config['parameters']['query'] = "SELECT 1 LIMIT 0";

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Invalid configuration for path "parameters": Incremental fetching is not supported for advanced query.'
        );

        $app = $this->getApp($config);
        $app->run();
    }

    public function testInvalidConfigsNeitherTableNorQueryWithNoName(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['name']);
        unset($config['parameters']['table']);

        $this->expectException(UserException::class);
        $this->expectExceptionMessageRegExp('(.*One of table or query is required.*)');

        ($this->getApp($config))->run();
    }

    public function testInvalidConfigsInvalidTableWithNoName(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['name']);
        $config['parameters']['table'] = ['tableName' => 'sales'];

        $this->expectException(UserException::class);
        $this->expectExceptionMessageRegExp('(.*The table property requires "tableName" and "schema".*)');

        ($this->getApp($config))->run();
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

    protected function createAutoIncrementAndTimestampTable(): void
    {
        $this->db->exec('DROP TABLE IF EXISTS auto_increment_timestamp');

        $this->db->exec(
            'CREATE TABLE auto_increment_timestamp (
            `id` INT NOT NULL AUTO_INCREMENT,
            `name` VARCHAR(30) NOT NULL DEFAULT \'pam\',
            `timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`)  
        )'
        );
        $this->db->exec('INSERT INTO auto_increment_timestamp (`name`) VALUES (\'george\'), (\'henry\')');
    }

    protected function assertExtractedData(
        string $expectedCsvFile,
        string $outputName,
        bool $headerExpected = true
    ): void {
        $outputCsvFile = $this->dataDir . '/out/tables/' . $outputName . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $outputName . '.csv.manifest';

        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
    }
}
