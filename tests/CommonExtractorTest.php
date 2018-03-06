<?php

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Yaml\Yaml;
use Keboola\DbExtractor\Test\ExtractorTest;
use Keboola\DbExtractor\Test\DataLoader;

class CommonExtractorTest extends ExtractorTest
{
    const DRIVER = 'common';

    /** @var  \PDO */
    private $db;

    public function setUp()
    {
        if (!defined('APP_NAME')) {
            define('APP_NAME', 'ex-db-common');
        }
        $this->initDatabase();
    }

    private function initDatabase()
    {
        $dataLoader = new DataLoader(
            $this->getEnv(self::DRIVER, 'DB_HOST'),
            $this->getEnv(self::DRIVER, 'DB_PORT'),
            $this->getEnv(self::DRIVER, 'DB_DATABASE'),
            $this->getEnv(self::DRIVER, 'DB_USER'),
            $this->getEnv(self::DRIVER, 'DB_PASSWORD')
        );

        $dataLoader->getPdo()->exec(sprintf("DROP DATABASE IF EXISTS `%s`", $this->getEnv(self::DRIVER, 'DB_DATABASE')));
        $dataLoader->getPdo()->exec(sprintf("
            CREATE DATABASE `%s`
            DEFAULT CHARACTER SET utf8
            DEFAULT COLLATE utf8_general_ci
        ", $this->getEnv(self::DRIVER, 'DB_DATABASE')));

        $dataLoader->getPdo()->exec("USE " . $this->getEnv(self::DRIVER, 'DB_DATABASE'));

        $dataLoader->getPdo()->exec("SET NAMES utf8;");
        $dataLoader->getPdo()->exec("CREATE TABLE escapingPK (
                                    col1 VARCHAR(155), 
                                    col2 VARCHAR(155), 
                                    PRIMARY KEY (col1, col2))");

        $dataLoader->getPdo()->exec("CREATE TABLE escaping (
                                  col1 VARCHAR(155) NOT NULL DEFAULT 'abc', 
                                  col2 VARCHAR(155) NOT NULL DEFAULT 'abc',
                                  FOREIGN KEY (col1, col2) REFERENCES escapingPK(col1, col2))");

        $inputFile = ROOT_PATH . '/tests/data/escaping.csv';
        $dataLoader->load($inputFile, 'escapingPK');
        $dataLoader->load($inputFile, 'escaping');

        // let other methods use the db connection
        $this->db = $dataLoader->getPdo();
    }

    public function testRun()
    {
        $this->assertRunResult((new Application($this->getConfig(self::DRIVER)))->run());
    }

    public function testRunJsonConfig()
    {
        $this->assertRunResult((new Application($this->getConfig(self::DRIVER, 'json')))->run());
    }

    public function testRunConfigRow()
    {
        $result = (new Application($this->getConfigRow(self::DRIVER)))->run();
        $this->assertEquals('success', $result['status']);
        $this->assertEquals('in.c-main.escaping', $result['imported']['outputTable']);
        $this->assertEquals(7, $result['imported']['rows']);
    }

    public function testRunWithSSH()
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey(self::DRIVER),
                'public' => $this->getEnv(self::DRIVER, 'DB_SSH_KEY_PUBLIC')
            ],
            'sshHost' => 'sshproxy'
        ];
        $this->assertRunResult((new Application($config))->run());
    }

    public function testRunWithSSHDeprecated()
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey(self::DRIVER),
                'public' => $this->getEnv(self::DRIVER, 'DB_SSH_KEY_PUBLIC')
            ],
            'sshHost' => 'sshproxy',
            'localPort' => '33306',
            'remoteHost' => 'mysql',
            'remotePort' => '3306',
        ];

        $result = (new Application($config))->run();
        $this->assertRunResult($result);
    }

    public function testRunWithSSHUserException()
    {
        $this->setExpectedException('Keboola\DbExtractor\Exception\UserException');

        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey(self::DRIVER),
                'public' => $this->getEnv(self::DRIVER, 'DB_SSH_KEY_PUBLIC')
            ],
            'sshHost' => 'wronghost',
            'localPort' => '33306',
            'remoteHost' => 'mysql',
            'remotePort' => '3306',
        ];

        (new Application($config))->run();
    }

    public function testRunWithWrongCredentials()
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['db']['host'] = 'somebulshit';
        $config['parameters']['db']['#password'] = 'somecrap';

        try {
            (new Application($config))->run();
            $this->fail("Wrong credentials must raise error.");
        } catch (\Keboola\DbExtractor\Exception\UserException $e) {
        }
    }

    public function testRetries()
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['tables'][0]['query'] = "SELECT * FROM `table_that_does_not_exist`";
        $config['parameters']['tables'][0]['retries'] = 3;

        try {
            (new Application($config))->run();
        } catch (UserException $e) {
            $this->assertContains('Tried 3 times', $e->getMessage());
        }
    }

    public function testRunEmptyQuery()
    {
        $outputCsvFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest';
        @unlink($outputCsvFile);
        @unlink($outputManifestFile);

        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['tables'][0]['query'] = "SELECT * FROM escaping WHERE col1 = '123'";

        $result = (new Application($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertFileNotExists($outputCsvFile);
        $this->assertFileNotExists($outputManifestFile);
    }

    public function testTestConnection()
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'testConnection';
        $config['parameters']['tables'] = [];
        $app = new Application($config);
        $res = $app->run();

        $this->assertEquals('success', $res['status']);
    }

    public function testTestConnectionFailInTheMiddle()
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['tables'][] = [
            'id' => 10,
            'name' => 'bad',
            'query' => 'KILL CONNECTION_ID();',
            'outputTable' => 'dummy'
        ];
        try {
            (new Application($config))->run();
            $this->fail("Failing query must raise exception.");
        } catch (\Keboola\DbExtractor\Exception\UserException $e) {
            // test that the error message contains the query name
            $this->assertContains('[bad]', $e->getMessage());
        }
    }

    public function testTestConnectionFailure()
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'testConnection';
        $config['parameters']['tables'] = [];
        $config['parameters']['db']['#password'] = 'bullshit';
        $app = new Application($config);
        $exceptionThrown = false;
        try {
            $app->run();
        } catch (\Keboola\DbExtractor\Exception\UserException $e) {
            $exceptionThrown = true;
        }

        $this->assertTrue($exceptionThrown);
    }

    public function testGetTablesAction()
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'getTables';

        $app = new Application($config);

        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);
        $this->assertEquals('success', $result['status']);
        $this->assertCount(2, $result['tables']);

        $this->assertGreaterThan(5, $result['tables'][0]['rowCount']);
        $this->assertLessThan(9, $result['tables'][0]['rowCount']);
        $this->assertGreaterThan(5, $result['tables'][1]['rowCount']);
        $this->assertLessThan(9, $result['tables'][1]['rowCount']);

        unset($result['tables'][0]['rowCount']);
        unset($result['tables'][1]['rowCount']);

        $expectedData = [
            [
                "name" => "escaping",
                "schema" => "testdb",
                "type" => "BASE TABLE",
                "columns" => [
                    [
                        "name" => "col1",
                        "type" => "varchar",
                        "primaryKey" => false,
                        "length" => "155",
                        "nullable" => false,
                        "default" => "abc",
                        "ordinalPosition" => "1"
                    ],[
                        "name" => "col2",
                        "type" => "varchar",
                        "primaryKey" => false,
                        "length" => "155",
                        "nullable" => false,
                        "default" => "abc",
                        "ordinalPosition" => "2"
                    ]
                ]
            ],[
                "name" => "escapingPK",
                "schema" => "testdb",
                "type" => "BASE TABLE",
                "columns" => [
                    [
                        "name" => "col1",
                        "type" => "varchar",
                        "primaryKey" => true,
                        "length" => "155",
                        "nullable" => false,
                        "default" => "",
                        "ordinalPosition" => "1"
                    ], [
                        "name" => "col2",
                        "type" => "varchar",
                        "primaryKey" => true,
                        "length" => "155",
                        "nullable" => false,
                        "default" => "",
                        "ordinalPosition" => "2"
                    ]
                ]
            ]
        ];
        $this->assertEquals($expectedData, $result['tables']);
    }

    public function testMetadataManifest()
    {
        $config = $this->getConfig(self::DRIVER);
        unset($config['parameters']['tables'][0]);

        $manifestFile = $this->dataDir . '/out/tables/in.c-main.simple.csv.manifest';
        @unlink($manifestFile);

        $app = new Application($config);

        $result = $app->run();
        $this->assertRunResult($result);

        $outputManifest = Yaml::parse(
            file_get_contents($manifestFile)
        );

        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);

        $expectedMetadata = [
            'KBC.name' => 'escaping',
            'KBC.schema' => 'testdb',
            'KBC.type' => 'BASE TABLE'
        ];
        $metadataList = [];
        foreach ($outputManifest['metadata'] as $i => $metadata) {
            $this->assertArrayHasKey('key', $metadata);
            $this->assertArrayHasKey('value', $metadata);
            $metadataList[$metadata['key']] = $metadata['value'];
        }

        $this->assertGreaterThan(5, $metadataList['KBC.rowCount']);
        $this->assertLessThan(9, $metadataList['KBC.rowCount']);
        unset($metadataList['KBC.rowCount']);

        $this->assertEquals($expectedMetadata, $metadataList);
        $this->assertArrayHasKey('column_metadata', $outputManifest);
        $this->assertCount(2, $outputManifest['column_metadata']);
        $this->assertArrayHasKey('col1', $outputManifest['column_metadata']);
        $this->assertArrayHasKey('col2', $outputManifest['column_metadata']);

        $expectedColumnMetadata = [
            'KBC.datatype.type' => 'varchar',
            'KBC.datatype.basetype' => 'STRING',
            'KBC.datatype.nullable' => false,
            'KBC.datatype.default' => 'abc',
            'KBC.datatype.length' => '155',
            'KBC.primaryKey' => false,
            'KBC.ordinalPosition' => 1,
            'KBC.foreignKeyRefSchema' => 'testdb',
            'KBC.foreignKeyRefTable' => 'escapingPK',
            'KBC.foreignKeyRefColumn' => 'col1',
            'KBC.constraintName' => 'escaping_ibfk_1'
        ];

        $colMetadata = [];
        foreach ($outputManifest['column_metadata']['col1'] as $metadata) {
            $this->assertArrayHasKey('key', $metadata);
            $this->assertArrayHasKey('value', $metadata);
            $colMetadata[$metadata['key']] = $metadata['value'];
        }
        $this->assertEquals($expectedColumnMetadata, $colMetadata);
    }

    public function testNonExistingAction()
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'sample';
        $config['parameters']['tables'] = [];

        try {
            $app = new Application($config);
            $app->run();

            $this->fail('Running non-existing actions should fail with UserException');
        } catch (\Keboola\DbExtractor\Exception\UserException $e) {
        }
    }

    public function testTableColumnsQuery()
    {
        $config = $this->getConfig(self::DRIVER);
        unset($config['parameters']['tables'][0]);

        $app = new Application($config);
        $result = $app->run();

        $this->assertRunResult($result);
    }

    public function testInvalidConfigurationQueryAndTable()
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['tables'][0]['table'] = ['schema' => 'testdb', 'tableName' => 'escaping'];
        try {
            $app = new Application($config);
            $app->run();
            $this->fail('table and query parameters cannot both be present');
        } catch (\Keboola\DbExtractor\Exception\UserException $e) {
            $this->assertStringStartsWith("Invalid Configuration", $e->getMessage());
        }
    }

    public function testInvalidConfigurationQueryNorTable()
    {
        $config = $this->getConfig(self::DRIVER);
        unset($config['parameters']['tables'][0]['query']);
        try {
            $app = new Application($config);
            $app->run();
            $this->fail('one of table or query is required');
        } catch (\Keboola\DbExtractor\Exception\UserException $e) {
            $this->assertStringStartsWith("Invalid Configuration", $e->getMessage());
        }
    }

    public function testStrangeTableName()
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['tables'][0]['outputTable'] = "in.c-main.something/ weird";
        unset($config['parameters']['tables'][1]);
        $result = (new Application($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($this->dataDir . '/out/tables/in.c-main.something-weird.csv');
        $this->assertFileExists($this->dataDir . '/out/tables/in.c-main.something-weird.csv.manifest');
    }

    public function testIncrementalFetching()
    {
        $config = $this->getIncrementalFetchingConfig();
        $this->createAutoIncrementAndTimestampTable();

        $result = (new Application($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2
            ],
            $result['imported']
        );

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertArrayHasKey('timestamp', $result['state']['lastFetchedRow']);
        $this->assertArrayHasKey('autoIncrement', $result['state']['lastFetchedRow']);
        $this->assertEquals(2, $result['state']['lastFetchedRow']['autoIncrement']);

        sleep(2);
        // the next fetch should be empty
        $emptyResult = (new Application($config, $result['state']))->run();
        $this->assertEquals(0, $emptyResult['imported']['rows']);

        sleep(2);
        //now add a couple rows and run it again.
        $this->db->exec('INSERT INTO auto_increment_timestamp (`name`) VALUES (\'charles\'), (\'william\')');

        $newResult = (new Application($config, $result['state']))->run();

        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertArrayHasKey('timestamp', $newResult['state']['lastFetchedRow']);
        $this->assertArrayHasKey('autoIncrement', $newResult['state']['lastFetchedRow']);
        $this->assertEquals(4, $newResult['state']['lastFetchedRow']['autoIncrement']);
        $this->assertGreaterThan(
            $result['state']['lastFetchedRow']['timestamp'],
            $newResult['state']['lastFetchedRow']['timestamp']
        );
    }

    public function testInvalidIncrementalFetchingColumns()
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetching'] = [
            'autoIncrementColumn' => 'fakeCol' // column does not exist
        ];
        try {
            $result = (new Application($config))->run();
            $this->fail('specified autoIncrement column does not exist, should fail.');
        } catch (UserException $e) {
            $this->assertStringStartsWith("Specified autoIncrement column", $e->getMessage());
        }

        $config['parameters']['incrementalFetching'] = [
            'timestampColumn' => 'fakeCol' // column does not exist
        ];
        try {
            $result = (new Application($config))->run();
            $this->fail('specified autoIncrement column does not exist, should fail.');
        } catch (UserException $e) {
            $this->assertStringStartsWith("Specified timestamp column", $e->getMessage());
        }
    }

    public function testInvalidIncrementalFetchingConfig()
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['query'] = 'SELECT * FROM auto_increment_timestamp';
        unset($config['parameters']['table']);

        try {
            $result = (new Application($config))->run();
            $this->fail('specified autoIncrement column does not exist, should fail.');
        } catch (UserException $e) {
            $this->assertStringStartsWith("Invalid Configuration", $e->getMessage());
        }
    }

    private function getIncrementalFetchingConfig()
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['query']);
        $config['parameters']['table'] = [
            'tableName' => 'auto_increment_timestamp',
            'schema' => 'testdb'
        ];
        $config['parameters']['incremental'] = true;
        $config['parameters']['name'] = 'auto-increment-timestamp';
        $config['parameters']['outputTable'] = 'in.c-main.auto-increment-timestamp';
        $config['parameters']['primaryKey'] = ['id'];
        $config['parameters']['incrementalFetching'] = [
            'autoIncrementColumn' => 'id',
            'timestampColumn' => 'timestamp'
        ];
        return $config;
    }

    protected function createAutoIncrementAndTimestampTable()
    {
        $this->db->exec('DROP TABLE IF EXISTS auto_increment_timestamp');

        $this->db->exec('CREATE TABLE auto_increment_timestamp (
            `id` INT NOT NULL AUTO_INCREMENT,
            `name` VARCHAR(30) NOT NULL DEFAULT \'pam\',
            `timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`)  
        )');
        $this->db->exec('INSERT INTO auto_increment_timestamp (`name`) VALUES (\'george\'), (\'henry\')');
    }

    protected function assertRunResult($result)
    {
        $expectedCsvFile = ROOT_PATH . '/tests/data/escaping.csv';
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
    }
}
