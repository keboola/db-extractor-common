<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Tests\Old;

use Keboola\Csv\CsvReader;
use Keboola\DbExtractorCommon\Exception\ApplicationException;
use Keboola\Component\UserException;
use Keboola\DbExtractorCommon\Tests\CommonExtractor;
use Keboola\DbExtractorCommon\Tests\DataLoader;
use Keboola\DbExtractorCommon\Tests\ExtractorTest;
use Keboola\Component\Logger;
use Monolog\Handler\TestHandler;
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
        $this->cleanInputDirectory();
        $this->cleanOutputDirectory();
        $this->initDatabase();
    }

    private function getApp(array $config, array $state = []): CommonExtractor
    {
        return parent::getCommonExtractor($config, $state);
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

    private function cleanInputDirectory(): void
    {
        $inputStateFile = $this->dataDir . '/in/state.json';
        if (file_exists($inputStateFile)) {
            unlink($inputStateFile);
        }
    }

    private function cleanOutputDirectory(): void
    {
        $configFilePath = $this->dataDir . DIRECTORY_SEPARATOR . 'config.json';
        if (file_exists($configFilePath)) {
            unlink($configFilePath);
        }

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
        $config = $this->getConfig(self::DRIVER);
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);
        $stdout = $this->runApplication($app);
        $result = json_decode($stdout, true);

        $this->assertExtractedData($this->dataDir . '/escaping.csv', $result['imported'][0]['outputTable']);
        $this->assertExtractedData($this->dataDir . '/simple.csv', $result['imported'][1]['outputTable']);

        $manifest = json_decode(
            (string) file_get_contents(sprintf(
                "%s/out/tables/%s.csv.manifest",
                $this->dataDir,
                $result['imported'][1]['outputTable']
            )),
            true
        );
        $this->assertEquals(["weird_I_d", 'S_oPaulo'], $manifest['columns']);
        $this->assertEquals(["weird_I_d"], $manifest['primary_key']);
    }

    public function testRunJsonConfig(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);
        $stdout = $this->runApplication($app);
        $result = json_decode($stdout, true);

        $this->assertExtractedData($this->dataDir . '/escaping.csv', $result['imported'][0]['outputTable']);
        $manifest = json_decode(
            (string) file_get_contents(sprintf(
                "%s/out/tables/%s.csv.manifest",
                $this->dataDir,
                $result['imported'][0]['outputTable']
            )),
            true
        );
        $this->assertArrayNotHasKey('columns', $manifest);
        $this->assertArrayNotHasKey('primary_key', $manifest);
        
        $this->assertExtractedData($this->dataDir . '/simple.csv', $result['imported'][1]['outputTable']);
        $manifest = json_decode(
            (string) file_get_contents(sprintf(
                "%s/out/tables/%s.csv.manifest",
                $this->dataDir,
                $result['imported'][1]['outputTable']
            )),
            true
        );
        $this->assertEquals(["weird_I_d", 'S_oPaulo'], $manifest['columns']);
        $this->assertEquals(["weird_I_d"], $manifest['primary_key']);
    }

    public function testRunConfigRow(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);
        $stdout = $this->runApplication($app);
        $result = json_decode($stdout, true);

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

    public function testRunWithSSH(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey(self::DRIVER),
                'public' => $this->getEnv(self::DRIVER, 'DB_SSH_KEY_PUBLIC'),
            ],
            'sshHost' => 'sshproxy',
        ];
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);
        $stdout = $this->runApplication($app);
        $result = json_decode($stdout, true);

        $this->assertExtractedData($this->dataDir . '/escaping.csv', $result['imported'][0]['outputTable']);
        $this->assertExtractedData($this->dataDir . '/simple.csv', $result['imported'][1]['outputTable']);
    }

    public function testRunWithSSHDeprecated(): void
    {
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
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);
        $stdout = $this->runApplication($app);
        $result = json_decode($stdout, true);

        $this->assertExtractedData($this->dataDir . '/escaping.csv', $result['imported'][0]['outputTable']);
        $this->assertExtractedData($this->dataDir . '/simple.csv', $result['imported'][1]['outputTable']);
    }

    public function testRunWithSSHUserException(): void
    {
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
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Unable to create ssh tunnel. Output:  ErrorOutput: ssh:'
            . ' Could not resolve hostname wronghost: Name or service not known'
        );
        $app->run();
    }

    public function testRunWithWrongCredentials(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['db']['host'] = 'somebulshit';
        $config['parameters']['db']['#password'] = 'somecrap';
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Error connecting to DB: SQLSTATE[HY000] [2002] php_network_getaddresses:'
            . ' getaddrinfo failed: Name or service not known'
        );
        $app->run();
    }

    public function testRetries(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['tables'][0]['query'] = "SELECT * FROM `table_that_does_not_exist`";
        $config['parameters']['tables'][0]['retries'] = 3;
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            '[in.c-main.escaping]: DB query failed: SQLSTATE[42S02]: Base table or view not found: 1146'
            . ' Table \'testdb.table_that_does_not_exist\' doesn\'t exist Tried 3 times.'
        );
        $app->run();
    }

    public function testRunEmptyQuery(): void
    {
        $outputCsvFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/in.c-main.escaping.csv.manifest';

        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['tables'][0]['query'] = "SELECT * FROM escaping WHERE col1 = '123'";
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);
        $stdout = $this->runApplication($app);
        $result = json_decode($stdout, true);

        $this->assertEquals('success', $result['status']);
        $this->assertFileNotExists($outputCsvFile);
        $this->assertFileNotExists($outputManifestFile);
    }

    public function testTestConnection(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'testConnection';
        $config['parameters']['tables'] = [];
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);
        $stdout = $this->runApplication($app);
        $result = json_decode($stdout, true);

        $this->assertEquals('success', $result['status']);
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
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            '[dummy]: DB query failed: SQLSTATE[70100]: <<Unknown error>>:'
            . ' 1317 Query execution was interrupted'
        );
        $app->run();
    }

    public function testTestConnectionFailure(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'testConnection';
        $config['parameters']['tables'] = [];
        $config['parameters']['db']['#password'] = 'bullshit';
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);

        $this->expectException(UserException::class);
        $this->expectExceptionMessageRegExp(
            '~Error connecting to DB: SQLSTATE\[HY000\] \[1045\] Access denied for user \'root\'~'
        );
        $app->run();
    }

    public function testGetTablesAction(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'getTables';
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);
        $stdout = $this->runApplication($app);
        $result = json_decode($stdout, true);

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
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);
        $stdout = $this->runApplication($app);
        $result = json_decode($stdout, true);

        $manifestFile = $this->dataDir . '/out/tables/in.c-main.simple.csv.manifest';

        $this->assertExtractedData($this->dataDir . '/simple.csv', $result['imported'][0]['outputTable']);

        $outputManifest = json_decode(
            (string) file_get_contents($manifestFile),
            true
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
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'sample';
        $config['parameters']['tables'] = [];
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Undefined action "sample".');
        $app->run();
    }

    public function testTableColumnsQuery(): void
    {
        $config = $this->getConfig(self::DRIVER);
        unset($config['parameters']['tables'][0]);
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);
        $stdout = $this->runApplication($app);
        $result = json_decode($stdout, true);

        $outputTableName = $result['imported'][0]['outputTable'];
        $this->assertExtractedData($this->dataDir . '/simple.csv', $outputTableName);
        $manifest = json_decode(
            (string) file_get_contents(sprintf(
                "%s/out/tables/%s.csv.manifest",
                $this->dataDir,
                $outputTableName
            )),
            true
        );
        $this->assertEquals(["weird_I_d", 'S_oPaulo'], $manifest['columns']);
        $this->assertEquals(["weird_I_d"], $manifest['primary_key']);
    }

    public function testInvalidConfigurationQueryAndTable(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['tables'][0]['table'] = ['schema' => 'testdb', 'tableName' => 'escaping'];
        $this->prepareConfigInDataDir($config);


        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Invalid configuration for path "root.parameters.tables": Both "table" and "query" cannot be set together.'
        );
        $this->getApp($config);
    }

    public function testInvalidConfigurationQueryNorTable(): void
    {
        $config = $this->getConfig(self::DRIVER);
        unset($config['parameters']['tables'][0]['query']);
        $this->prepareConfigInDataDir($config);


        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Invalid configuration for path "root.parameters.tables": Either "table" or "query" must be defined'
        );
        $this->getApp($config);
    }

    public function testStrangeTableName(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['tables'][0]['outputTable'] = "in.c-main.something/ weird";
        unset($config['parameters']['tables'][1]);
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);
        $stdout = $this->runApplication($app);
        $result = json_decode($stdout, true);

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($this->dataDir . '/out/tables/in.c-main.something-weird.csv');
        $this->assertFileExists($this->dataDir . '/out/tables/in.c-main.something-weird.csv.manifest');
    }

    public function testIncrementalFetchingByTimestamp(): void
    {
        $this->createAutoIncrementAndTimestampTable();

        $config = $this->getIncrementalFetchingConfig();
        $config['incrementalFethcingColumn'] = 'timestamp';
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);
        $stdout = $this->runApplication($app);
        $result = json_decode($stdout, true);

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
        $app = $this->getApp($config, $result['state']);
        $stdout = $this->runApplication($app);
        $emptyResult = json_decode($stdout, true);

        $this->assertEquals(0, $emptyResult['imported']['rows']);

        sleep(2);
        //now add a couple rows and run it again.
        $this->db->exec('INSERT INTO auto_increment_timestamp (`name`) VALUES (\'charles\'), (\'william\')');

        $app = $this->getApp($config, $result['state']);
        $stdout = $this->runApplication($app);
        $newResult = json_decode($stdout, true);

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
        $this->createAutoIncrementAndTimestampTable();

        $config = $this->getIncrementalFetchingConfig();
        $config['incrementalFethcingColumn'] = 'id';
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);
        $stdout = $this->runApplication($app);
        $result = json_decode($stdout, true);

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

        $app = $this->getApp($config, $result['state']);
        $stdout = $this->runApplication($app);
        $emptyResult = json_decode($stdout, true);

        $this->assertEquals(0, $emptyResult['imported']['rows']);

        sleep(2);
        //now add a couple rows and run it again.
        $this->db->exec('INSERT INTO auto_increment_timestamp (`name`) VALUES (\'charles\'), (\'william\')');

        $app = $this->getApp($config, $result['state']);
        $stdout = $this->runApplication($app);
        $newResult = json_decode($stdout, true);


        //check that output state contains expected information
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertEquals(4, $newResult['state']['lastFetchedRow']);
        $this->assertEquals(2, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingLimit(): void
    {
        $this->createAutoIncrementAndTimestampTable();

        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingLimit'] = 1;
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);
        $stdout = $this->runApplication($app);
        $result = json_decode($stdout, true);

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
        $app = $this->getApp($config, $result['state']);
        $stdout = $this->runApplication($app);
        $result = json_decode($stdout, true);

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
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);
        $stdout = $this->runApplication($app);
        $result = json_decode($stdout, true);

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

    public function testIncrementalFetchingOnInvalidColumnName(): void
    {
        $this->createAutoIncrementAndTimestampTable();

        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'fakeCol'; // column does not exist
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Column [fakeCol] specified for incremental fetching was not found in the table'
        );
        $app->run();
    }

    public function testIncrementalFetchingOnInvalidColumnAttributes(): void
    {
        $this->createAutoIncrementAndTimestampTable();

        $config = $this->getIncrementalFetchingConfig();

        // column exists but is not auto-increment nor updating timestamp so should fail
        $config['parameters']['incrementalFetchingColumn'] = 'name';
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Column [name] specified for incremental fetching is not an auto increment column'
            . ' or an auto update timestamp'
        );
        $app->run();
    }

    public function testIncrementalFetchingInvalidConfig(): void
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['query'] = 'SELECT * FROM auto_increment_timestamp';
        unset($config['parameters']['table']);
        $this->prepareConfigInDataDir($config);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Invalid configuration for path "root.parameters":'
            . ' Incremental fetching is not supported for advanced queries.'
        );
        $this->getApp($config);
    }

    public function testColumnOrdering(): void
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['columns'] = ['timestamp', 'id', 'name'];
        $config['parameters']['outputTable'] = 'in.c-main.columnsCheck';
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);
        $stdout = $this->runApplication($app);
        $result = json_decode($stdout, true);

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
            ],
        ];
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);
        $stdout = $this->runApplication($app);
        $result = json_decode($stdout, true);

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
        $this->prepareConfigInDataDir($config);

        $app = $this->getApp($config);
        $stdout = $this->runApplication($app);
        $result = json_decode($stdout, true);

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testInvalidConfigsBothTableAndQueryWithNoName(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['name']);

        // we want to test the no results case
        $config['parameters']['query'] = "SELECT 1 LIMIT 0";
        $this->prepareConfigInDataDir($config);


        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Invalid configuration for path "root.parameters": Both "table" and "query" cannot be set together.'
        );
        $this->getApp($config);
    }

    public function testInvalidConfigsBothIncrFetchAndQueryWithNoName(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['name']);
        unset($config['parameters']['table']);
        $config['parameters']['incrementalFetchingColumn'] = 'abc';

        // we want to test the no results case
        $config['parameters']['query'] = "SELECT 1 LIMIT 0";
        $this->prepareConfigInDataDir($config);

        $this->expectException(UserException::class);
        $this->expectExceptionMessageRegExp('(.*Incremental fetching is not supported for advanced queries.*)');
        $this->getApp($config);
    }

    public function testInvalidConfigsNeitherTableNorQueryWithNoName(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['name']);
        unset($config['parameters']['table']);
        $this->prepareConfigInDataDir($config);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Invalid configuration for path "root.parameters": Either "table" or "query" must be defined.'
        );
        $this->getApp($config);
    }

    public function testInvalidConfigsInvalidTableWithNoName(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['name']);
        $config['parameters']['table'] = ['tableName' => 'sales'];
        $this->prepareConfigInDataDir($config);


        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'The child node "schema" at path "root.parameters.table" must be configured.'
        );
        $this->getApp($config);
    }

    public function testNoRetryOnCsvError(): void
    {
        $config = $this->getConfigRowForCsvErr(self::DRIVER);
        $this->prepareConfigInDataDir($config);

        (new Filesystem)->remove($this->dataDir . '/out/tables/in.c-main.simple-csv-err.csv');
        (new Filesystem)->symlink('/dev/full', $this->dataDir . '/out/tables/in.c-main.simple-csv-err.csv');

        $handler = new TestHandler();
        $logger = new Logger();
        $logger->pushHandler($handler);
        putenv(sprintf('KBC_DATADIR=%s', $this->dataDir));
        $app = new CommonExtractor($logger);
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
