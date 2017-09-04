<?php

use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Yaml\Yaml;

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 12/02/16
 * Time: 16:39
 */
class CommonExtractorTest extends ExtractorTest
{
    public function setUp()
    {
        if (!defined('APP_NAME')) {
            define('APP_NAME', 'ex-db-common');
        }

        $inputFile = ROOT_PATH . '/tests/data/escaping.csv';

        $dataLoader = new \Keboola\DbExtractor\Test\DataLoader(
            $this->getEnv('common', 'DB_HOST'),
            $this->getEnv('common', 'DB_PORT'),
            $this->getEnv('common', 'DB_DATABASE'),
            $this->getEnv('common', 'DB_USER'),
            $this->getEnv('common', 'DB_PASSWORD')
        );

        $dataLoader->getPdo()->exec(sprintf("DROP DATABASE IF EXISTS `%s`", $this->getEnv('common', 'DB_DATABASE')));
        $dataLoader->getPdo()->exec(sprintf("
            CREATE DATABASE `%s`
            DEFAULT CHARACTER SET utf8
            DEFAULT COLLATE utf8_general_ci
        ", $this->getEnv('common', 'DB_DATABASE')));

        $dataLoader->getPdo()->exec("USE " . $this->getEnv('common', 'DB_DATABASE'));

        $dataLoader->getPdo()->exec("SET NAMES utf8;");
        $dataLoader->getPdo()->exec("DROP TABLE IF EXISTS escaping");
        $dataLoader->getPdo()->exec("DROP TABLE IF EXISTS escapingPK");
        $dataLoader->getPdo()->exec("CREATE TABLE escapingPK (
                                    col1 VARCHAR(155), 
                                    col2 VARCHAR(155), 
                                    PRIMARY KEY (col1, col2))");

        $dataLoader->getPdo()->exec("CREATE TABLE escaping (
                                  col1 VARCHAR(155) NOT NULL DEFAULT 'abc', 
                                  col2 VARCHAR(155) NOT NULL DEFAULT 'abc',
                                  FOREIGN KEY (col1, col2) REFERENCES escapingPK(col1, col2))");

        $dataLoader->load($inputFile, 'escapingPK');
        $dataLoader->load($inputFile, 'escaping');
    }

    public function getConfig($driver = 'common')
    {
        $config = parent::getConfig($driver);
        $config['parameters']['extractor_class'] = 'Common';
        return $config;
    }

    public function testRun()
    {
        $this->assertRunResult((new Application($this->getConfig()))->run());
    }

    public function testRunWithSSH()
    {
        $config = $this->getConfig();
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getEnv('common', 'DB_SSH_KEY_PRIVATE'),
                'public' => $this->getEnv('common', 'DB_SSH_KEY_PUBLIC')
            ],
            'sshHost' => 'sshproxy'
        ];
        $this->assertRunResult((new Application($config))->run());
    }

    public function testRunWithSSHDeprecated()
    {
        $config = $this->getConfig();
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getEnv('common', 'DB_SSH_KEY_PRIVATE'),
                'public' => $this->getEnv('common', 'DB_SSH_KEY_PUBLIC')
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

        $config = $this->getConfig();
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getEnv('common', 'DB_SSH_KEY_PRIVATE'),
                'public' => $this->getEnv('common', 'DB_SSH_KEY_PUBLIC')
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
        $config = $this->getConfig();
        $config['parameters']['db']['host'] = 'somebulshit';
        $config['parameters']['db']['#password'] = 'somecrap';

        try {
            (new Application($config))->run();
            $this->fail("Wrong credentials must raise error.");
        } catch (\Keboola\DbExtractor\Exception\UserException $e) {
        }
    }

    public function testRunEmptyQuery()
    {
        $outputCsvFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest';
        @unlink($outputCsvFile);
        @unlink($outputManifestFile);

        $config = $this->getConfig();
        $config['parameters']['tables'][0]['query'] = "SELECT * FROM escaping WHERE col1 = '123'";

        $result = (new Application($config))->run();

        $this->assertEquals('success', $result['status']);
        $this->assertFileNotExists($outputCsvFile);
        $this->assertFileNotExists($outputManifestFile);
    }

    public function testTestConnection()
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        unset($config['parameters']['tables']);
        $app = new Application($config);
        $res = $app->run();

        $this->assertEquals('success', $res['status']);
    }

    public function testTestConnectionFailInTheMiddle()
    {
        $config = $this->getConfig();
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
        }
    }

    public function testTestConnectionFailure()
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        unset($config['parameters']['tables']);
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
        $config = $this->getConfig();
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
                        "ordinalPosition" => "1",
                        "constraintName" => "escaping_ibfk_1",
                        "foreignKeyRefSchema" => "testdb",
                        "foreignKeyRefTable" => "escapingPK",
                        "foreignKeyRefColumn" => "col1"
                    ],[
                        "name" => "col2",
                        "type" => "varchar",
                        "primaryKey" => false,
                        "length" => "155",
                        "nullable" => false,
                        "default" => "abc",
                        "ordinalPosition" => "2",
                        "constraintName" => "escaping_ibfk_1",
                        "foreignKeyRefSchema" => "testdb",
                        "foreignKeyRefTable" => "escapingPK",
                        "foreignKeyRefColumn" => "col2"
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
                        "ordinalPosition" => "1",
                        "constraintName" => "PRIMARY"
                    ], [
                        "name" => "col2",
                        "type" => "varchar",
                        "primaryKey" => true,
                        "length" => "155",
                        "nullable" => false,
                        "default" => "",
                        "ordinalPosition" => "2",
                        "constraintName" => "PRIMARY"
                    ]
                ]
            ]
        ];
        $this->assertEquals($expectedData, $result['tables']);
    }

    public function testMetadataManifest()
    {
        $config = $this->getConfig();
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
        $config = $this->getConfig();
        $config['action'] = 'sample';
        unset($config['parameters']['tables']);

        try {
            $app = new Application($config);
            $app->run();

            $this->fail('Running non-existing actions should fail with UserException');
        } catch (\Keboola\DbExtractor\Exception\UserException $e) {
        }
    }

    public function testTableColumnsQuery()
    {
        $config = $this->getConfig();
        unset($config['parameters']['tables'][0]);

        $app = new Application($config);
        $result = $app->run();

        $this->assertRunResult($result);
    }

    public function testInvalidConfigurationQueryAndTable()
    {
        $config = $this->getConfig();
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
        $config = $this->getConfig();
        unset($config['parameters']['tables'][0]['query']);
        try {
            $app = new Application($config);
            $app->run();
            $this->fail('one of table or query is required');
        } catch (\Keboola\DbExtractor\Exception\UserException $e) {
            $this->assertStringStartsWith("Invalid Configuration", $e->getMessage());
        }
    }

    protected function assertRunResult($result)
    {
        $expectedCsvFile = ROOT_PATH . '/tests/data/escaping.csv';
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
    }
}
