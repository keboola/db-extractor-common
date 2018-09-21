<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvReader;
use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Test\DataLoader;
use Keboola\DbExtractor\Test\ExtractorTest;
use Keboola\DbExtractor\Logger;
use Monolog\Handler\TestHandler;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Yaml\Yaml;

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

        $dataLoader->getPdo()->exec(
            sprintf(
                "DROP DATABASE IF EXISTS `%s`",
                $this->getEnv(self::DRIVER, 'DB_DATABASE')
            )
        );
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

        $inputFile = $this->dataDir . '/escaping.csv';
        $simpleFile = $this->dataDir . '/simple.csv';
        $dataLoader->load($inputFile, 'escapingPK');
        $dataLoader->load($inputFile, 'escaping');
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

    public function testRunSimple(): void
    {
        $this->cleanOutputDirectory();
        $result = ($this->getApp($this->getConfig(self::DRIVER)))->run();
        $this->assertExtractedData($this->dataDir . '/escaping.csv', $result['imported'][0]['outputTable']);
        $this->assertExtractedData($this->dataDir . '/simple.csv', $result['imported'][1]['outputTable']);
        $manifest = json_decode(
            (string) file_get_contents(
                sprintf("%s/out/tables/%s.csv.manifest", $this->dataDir, $result['imported'][1]['outputTable'])
            ),
            true
        );
        $this->assertEquals(["weird_I_d", 'S_oPaulo'], $manifest['columns']);
        $this->assertEquals(["weird_I_d"], $manifest['primary_key']);
    }

    public function testRunJsonConfig(): void
    {
        $this->cleanOutputDirectory();
        $result = ($this->getApp($this->getConfig(self::DRIVER, parent::CONFIG_FORMAT_JSON)))->run();

        $this->assertExtractedData($this->dataDir . '/escaping.csv', $result['imported'][0]['outputTable']);
        $manifest = json_decode(
            (string) file_get_contents(
                sprintf("%s/out/tables/%s.csv.manifest", $this->dataDir, $result['imported'][0]['outputTable'])
            ),
            true
        );
        $this->assertArrayNotHasKey('columns', $manifest);
        $this->assertArrayNotHasKey('primary_key', $manifest);
        
        $this->assertExtractedData($this->dataDir . '/simple.csv', $result['imported'][1]['outputTable']);
        $manifest = json_decode(
            (string) file_get_contents(
                sprintf("%s/out/tables/%s.csv.manifest", $this->dataDir, $result['imported'][1]['outputTable'])
            ),
            true
        );
        $this->assertEquals(["weird_I_d", 'S_oPaulo'], $manifest['columns']);
        $this->assertEquals(["weird_I_d"], $manifest['primary_key']);
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
            (string) file_get_contents(
                sprintf("%s/out/tables/%s.csv.manifest", $this->dataDir, $result['imported']['outputTable'])
            ),
            true
        );
        $this->assertEquals(["weird_I_d", 'S_oPaulo'], $manifest['columns']);
        $this->assertEquals(["weird_I_d"], $manifest['primary_key']);
    }

    public function testRunWithSSH(): void
    {
        $this->cleanOutputDirectory();
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey(self::DRIVER),
                'public' => $this->getEnv(self::DRIVER, 'DB_SSH_KEY_PUBLIC'),
            ],
            'sshHost' => 'sshproxy',
        ];
        $result = ($this->getApp($config))->run();
        $this->assertExtractedData($this->dataDir . '/escaping.csv', $result['imported'][0]['outputTable']);
        $this->assertExtractedData($this->dataDir . '/simple.csv', $result['imported'][1]['outputTable']);
    }

    public function testRunWithSSHDeprecated(): void
    {
        $this->cleanOutputDirectory();
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey(self::DRIVER),
                'public' => $this->getEnv(self::DRIVER, 'DB_SSH_KEY_PUBLIC'),
            ],
            'sshHost' => 'sshproxy',
            'localPort' => '33306',
            'remoteHost' => 'mysql',
            'remotePort' => '3306',
        ];

        $result = ($this->getApp($config))->run();
        $this->assertExtractedData($this->dataDir . '/escaping.csv', $result['imported'][0]['outputTable']);
        $this->assertExtractedData($this->dataDir . '/simple.csv', $result['imported'][1]['outputTable']);
    }

    public function testRunWithSSHUserException(): void
    {
        $this->cleanOutputDirectory();
        $this->expectException(UserException::class);

        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey(self::DRIVER),
                'public' => $this->getEnv(self::DRIVER, 'DB_SSH_KEY_PUBLIC'),
            ],
            'sshHost' => 'wronghost',
            'localPort' => '33306',
            'remoteHost' => 'mysql',
            'remotePort' => '3306',
        ];

        ($this->getApp($config))->run();
    }

    public function testRunWithWrongCredentials(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['db']['host'] = 'somebulshit';
        $config['parameters']['db']['#password'] = 'somecrap';

        $this->expectException(UserException::class);
        $app = $this->getApp($config);
        $app->run();
    }

    public function testRetries(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['tables'][0]['query'] = "SELECT * FROM `table_that_does_not_exist`";
        $config['parameters']['tables'][0]['retries'] = 3;

        try {
            ($this->getApp($config))->run();
        } catch (UserException $e) {
            $this->assertContains('Tried 3 times', $e->getMessage());
        }
    }

    public function testRunEmptyQuery(): void
    {
        $outputCsvFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest';

        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['tables'][0]['query'] = "SELECT * FROM escaping WHERE col1 = '123'";

        $result = ($this->getApp($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertFileNotExists($outputCsvFile);
        $this->assertFileNotExists($outputManifestFile);
    }

    public function testTestConnection(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'testConnection';
        $config['parameters']['tables'] = [];
        $app = $this->getApp($config);
        $res = $app->run();

        $this->assertEquals('success', $res['status']);
    }

    public function testTestConnectionFailInTheMiddle(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['tables'][] = [
            'id' => 10,
            'name' => 'bad',
            'query' => 'KILL CONNECTION_ID();',
            'outputTable' => 'dummy',
        ];
        try {
            ($this->getApp($config))->run();
            $this->fail("Failing query must raise exception.");
        } catch (\Keboola\DbExtractor\Exception\UserException $e) {
            // test that the error message contains the query name
            $this->assertContains('[dummy]', $e->getMessage());
        }
    }

    public function testTestConnectionFailure(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'testConnection';
        $config['parameters']['tables'] = [];
        $config['parameters']['db']['#password'] = 'bullshit';
        $app = $this->getApp($config);
        $exceptionThrown = false;
        try {
            $app->run();
        } catch (\Keboola\DbExtractor\Exception\UserException $e) {
            $exceptionThrown = true;
        }

        $this->assertTrue($exceptionThrown);
    }

    public function testGetTablesAction(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'getTables';

        $app = $this->getApp($config);

        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);
        $this->assertEquals('success', $result['status']);
        $this->assertCount(3, $result['tables']);

        unset($result['tables'][0]['rowCount']);
        unset($result['tables'][1]['rowCount']);
        unset($result['tables'][3]['rowCount']);

        $expectedData = array (
            0 =>
                array (
                    'name' => 'escaping',
                    'sanitizedName' => 'escaping',
                    'schema' => 'testdb',
                    'type' => 'BASE TABLE',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'col1',
                                    'sanitizedName' => 'col1',
                                    'type' => 'varchar',
                                    'primaryKey' => false,
                                    'length' => '155',
                                    'nullable' => false,
                                    'default' => 'abc',
                                    'ordinalPosition' => '1',
                                ),
                            1 =>
                                array (
                                    'name' => 'col2',
                                    'sanitizedName' => 'col2',
                                    'type' => 'varchar',
                                    'primaryKey' => false,
                                    'length' => '155',
                                    'nullable' => false,
                                    'default' => 'abc',
                                    'ordinalPosition' => '2',
                                ),
                        ),
                ),
            1 =>
                array (
                    'name' => 'escapingPK',
                    'sanitizedName' => 'escapingPK',
                    'schema' => 'testdb',
                    'type' => 'BASE TABLE',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => 'col1',
                                    'sanitizedName' => 'col1',
                                    'type' => 'varchar',
                                    'primaryKey' => true,
                                    'length' => '155',
                                    'nullable' => false,
                                    'default' => '',
                                    'ordinalPosition' => '1',
                                ),
                            1 =>
                                array (
                                    'name' => 'col2',
                                    'sanitizedName' => 'col2',
                                    'type' => 'varchar',
                                    'primaryKey' => true,
                                    'length' => '155',
                                    'nullable' => false,
                                    'default' => '',
                                    'ordinalPosition' => '2',
                                ),
                        ),
                ),
            2 =>
                array (
                    'name' => 'simple',
                    'sanitizedName' => 'simple',
                    'schema' => 'testdb',
                    'type' => 'BASE TABLE',
                    'rowCount' => '2',
                    'columns' =>
                        array (
                            0 =>
                                array (
                                    'name' => '_weird-I-d',
                                    'sanitizedName' => 'weird_I_d',
                                    'type' => 'varchar',
                                    'primaryKey' => true,
                                    'length' => '155',
                                    'nullable' => false,
                                    'default' => 'abc',
                                    'ordinalPosition' => '1',
                                ),
                            1 =>
                                array (
                                    'name' => 'SãoPaulo',
                                    'sanitizedName' => 'S_oPaulo',
                                    'type' => 'varchar',
                                    'primaryKey' => false,
                                    'length' => '155',
                                    'nullable' => false,
                                    'default' => 'abc',
                                    'ordinalPosition' => '2',
                                ),
                        ),
                ),
        );

        $this->assertEquals($expectedData, $result['tables']);
    }

    public function testMetadataManifest(): void
    {
        $config = $this->getConfig(self::DRIVER);
        unset($config['parameters']['tables'][0]);

        $manifestFile = $this->dataDir . '/out/tables/in.c-main.simple.csv.manifest';

        $app = $this->getApp($config);

        $result = $app->run();
        $this->assertExtractedData($this->dataDir . '/simple.csv', $result['imported'][0]['outputTable']);

        $outputManifest = Yaml::parse(
            (string) file_get_contents($manifestFile)
        );

        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);

        $expectedMetadata = [
            'KBC.name' => 'simple',
            'KBC.schema' => 'testdb',
            'KBC.type' => 'BASE TABLE',
            'KBC.sanitizedName' => 'simple',
        ];
        $metadataList = [];
        foreach ($outputManifest['metadata'] as $i => $metadata) {
            $this->assertArrayHasKey('key', $metadata);
            $this->assertArrayHasKey('value', $metadata);
            $metadataList[$metadata['key']] = $metadata['value'];
        }

        $this->assertEquals(2, $metadataList['KBC.rowCount']);
        unset($metadataList['KBC.rowCount']);

        $this->assertEquals($expectedMetadata, $metadataList);
        $this->assertArrayHasKey('column_metadata', $outputManifest);
        $this->assertCount(2, $outputManifest['column_metadata']);
        $this->assertArrayHasKey('weird_I_d', $outputManifest['column_metadata']);
        $this->assertArrayHasKey('S_oPaulo', $outputManifest['column_metadata']);

        $expectedColumnMetadata = array (
            'weird_I_d' =>
                array (
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
                    9 =>
                        array (
                            'key' => 'KBC.constraintName',
                            'value' => 'PRIMARY',
                        ),
                ),
            'S_oPaulo' =>
                array (
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
                            'value' => 'SãoPaulo',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.sanitizedName',
                            'value' => 'S_oPaulo',
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '2',
                        ),
                ),
        );

        $this->assertEquals($expectedColumnMetadata, $outputManifest['column_metadata']);
    }

    public function testNonExistingAction(): void
    {
        $this->doesNotPerformAssertions();
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'sample';
        $config['parameters']['tables'] = [];

        $this->expectException(UserException::class);
        $app = $this->getApp($config);
        $app->run();
    }

    public function testTableColumnsQuery(): void
    {
        $config = $this->getConfig(self::DRIVER);
        unset($config['parameters']['tables'][0]);

        $app = $this->getApp($config);
        $result = $app->run();

        $outputTableName = $result['imported'][0]['outputTable'];
        $this->assertExtractedData($this->dataDir . '/simple.csv', $outputTableName);
        $manifest = json_decode(
            (string) file_get_contents(sprintf("%s/out/tables/%s.csv.manifest", $this->dataDir, $outputTableName)),
            true
        );
        $this->assertEquals(["weird_I_d", 'S_oPaulo'], $manifest['columns']);
        $this->assertEquals(["weird_I_d"], $manifest['primary_key']);
    }

    public function testInvalidConfigurationQueryAndTable(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['tables'][0]['table'] = ['schema' => 'testdb', 'tableName' => 'escaping'];
        try {
            $app = $this->getApp($config);
            $app->run();
            $this->fail('table and query parameters cannot both be present');
        } catch (\Keboola\DbExtractor\Exception\UserException $e) {
            $this->assertStringStartsWith("Invalid Configuration", $e->getMessage());
        }
    }

    public function testInvalidConfigurationQueryNorTable(): void
    {
        $config = $this->getConfig(self::DRIVER);
        unset($config['parameters']['tables'][0]['query']);
        try {
            $app = $this->getApp($config);
            $app->run();
            $this->fail('one of table or query is required');
        } catch (\Keboola\DbExtractor\Exception\UserException $e) {
            $this->assertStringStartsWith("Invalid Configuration", $e->getMessage());
        }
    }

    public function testStrangeTableName(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['tables'][0]['outputTable'] = "in.c-main.something/ weird";
        unset($config['parameters']['tables'][1]);
        $result = ($this->getApp($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($this->dataDir . '/out/tables/in.c-main.something-weird.csv');
        $this->assertFileExists($this->dataDir . '/out/tables/in.c-main.something-weird.csv.manifest');
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

        try {
            $result = ($this->getApp($config))->run();
            $this->fail('cannot use incremental fetching with advanced query, should fail.');
        } catch (UserException $e) {
            $this->assertStringStartsWith("Invalid Configuration", $e->getMessage());
        }
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

        $outputManifest = json_decode((string) file_get_contents($outputManifestFile), true);

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
        $this->expectExceptionMessageRegExp('(.*Incremental fetching is not supported for advanced queries.*)');

        ($this->getApp($config))->run();
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

    public function testNoRetryOnCsvError(): void
    {
        $config = $this->getConfigRowForCsvErr(self::DRIVER);

        (new Filesystem)->remove($this->dataDir . '/out/tables/in.c-main.simple-csv-err.csv');
        (new Filesystem)->symlink('/dev/full', $this->dataDir . '/out/tables/in.c-main.simple-csv-err.csv');

        $handler = new TestHandler();
        $logger = new Logger($this->appName);
        $logger->pushHandler($handler);
        $app = new Application($config, $logger, []);
        try {
            $app->run();
            self::fail('Must raise exception');
        } catch (ApplicationException $e) {
            self::assertContains('Failed writing CSV File', $e->getMessage());
            self::assertFalse($handler->hasInfoThatContains('Retrying'));
        }
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
