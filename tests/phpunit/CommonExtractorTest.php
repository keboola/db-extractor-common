<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\CommonExceptions\ApplicationExceptionInterface;
use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\Component\Exception\BaseComponentException;
use Keboola\Component\JsonHelper;
use Keboola\Component\UserException;
use Keboola\Csv\CsvReader;
use Keboola\DbExtractor\Test\ExtractorTest;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PDO;
use PHPUnit\Framework\Assert;
use Psr\Log\Test\TestLogger;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use function PHPUnit\Framework\assertEquals;

class CommonExtractorTest extends ExtractorTest
{
    use TestDataTrait;

    public const DRIVER = 'common';

    protected PDO $db;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanStateFiles();
        $this->initDatabase();
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

        Assert::assertIsArray($manifest);
        Assert::assertEquals(['weird_I_d', 'SaoPaulo'], array_column($manifest['schema'], 'name'));
        Assert::assertTrue($manifest['schema'][0]['primary_key']);
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
        Assert::assertIsArray($manifest);
        Assert::assertEquals(['weird_I_d', 'SaoPaulo'], array_column($manifest['schema'], 'name'));
        Assert::assertTrue($manifest['schema'][0]['primary_key']);
        Assert::assertTrue($logger->hasInfoThatContains('Running query "TRUNCATE TABLE `simple`".'));
        Assert::assertTrue($logger->hasWarningThatContains('Exported "0" rows to "in.c-main.simple".'));
    }

    public function testFailingUserInitQueries(): void
    {
        $this->cleanOutputDirectory();
        $logger = new TestLogger();
        $config = $this->getConfigRow(self::DRIVER);
        $config['parameters']['db']['initQueries'] = [
            'failed user init query',
        ];
        $app = $this->getApp($config, $logger);
        try {
            $app->execute();
            Assert::fail('Failing query must raise exception.');
        } catch (UserExceptionInterface $e) {
            Assert::assertStringContainsString('Syntax error or access violation', $e->getMessage());
            Assert::assertStringContainsString('syntax to use near \'failed user init query\'', $e->getMessage());
        }
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
        Assert::assertIsArray($manifest);
        Assert::assertEquals(['weird_I_d', 'SaoPaulo'], array_column($manifest['schema'], 'name'));
        Assert::assertFalse($manifest['schema'][0]['primary_key']);
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
        Assert::assertIsArray($manifest);
        Assert::assertEquals(['weird_I_d', 'SaoPaulo'], array_column($manifest['schema'], 'name'));
        Assert::assertTrue($manifest['schema'][1]['primary_key']);
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

        Assert::assertIsArray($manifest);
        Assert::assertArrayNotHasKey('schema', $manifest);

        $filename = $this->dataDir . '/out/tables/in.c-main.simple.csv.manifest';
        $manifest = json_decode(
            (string) file_get_contents($filename),
            true,
        );
        Assert::assertIsArray($manifest);
        Assert::assertEquals(['weird_I_d', 'SaoPaulo'], array_column($manifest['schema'], 'name'));
        Assert::assertTrue($manifest['schema'][0]['primary_key']);
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
        Assert::assertIsArray($manifest);
        Assert::assertEquals(['weird_I_d', 'SaoPaulo'], array_column($manifest['schema'], 'name'));
        Assert::assertTrue($manifest['schema'][0]['primary_key']);
    }

    public function testRunWithSSH(): void
    {
        $this->cleanOutputDirectory();
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'user' => $config['parameters']['db']['user'],
            'keys' => [
                '#private' => $this->getPrivateKey(),
                'public' => $this->getPublicKey(),
            ],
            'sshHost' => 'sshproxy',
        ];
        $logger = new TestLogger();

        $this->getApp($config, $logger)->execute();
        Assert::assertTrue($logger->hasInfo('Exporting "simple" to "in.c-main.simple".'));
        Assert::assertTrue($logger->hasInfo('Exporting "escaping" to "in.c-main.escaping".'));

        // Connecting to SSH proxy, not to database directly
        $this->assertTrue($logger->hasInfoThatContains("Creating SSH tunnel to 'sshproxy' on local port '33006'"));
        $this->assertTrue($logger->hasInfoThatContains('Creating PDO connection to "mysql:host=127.0.0.1;port=33006'));
    }

    public function testRunWithSSHDeprecated(): void
    {
        $this->cleanOutputDirectory();
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey(),
                'public' => $this->getPublicKey(),
            ],
            'user' => $config['parameters']['db']['user'],
            'sshHost' => 'sshproxy',
            'localPort' => '12345',
            'remoteHost' => 'mysql',
            'remotePort' => '3306',
        ];

        $logger = new TestLogger();

        $this->getApp($config, $logger)->execute();
        Assert::assertTrue($logger->hasInfo('Exporting "escaping" to "in.c-main.escaping".'));
        Assert::assertTrue($logger->hasInfo('Exporting "simple" to "in.c-main.simple".'));

        // Connecting to SSH proxy, not to database directly
        $this->assertTrue($logger->hasInfoThatContains("Creating SSH tunnel to 'sshproxy' on local port '12345'"));
        $this->assertTrue($logger->hasInfoThatContains('Creating PDO connection to "mysql:host=127.0.0.1;port=12345'));
    }

    public function testRunWithSSHUserException(): void
    {
        $this->cleanOutputDirectory();
        $this->expectException(UserExceptionInterface::class);

        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey(),
                'public' => $this->getPublicKey(),
            ],
            'user' => $config['parameters']['db']['user'],
            'sshHost' => 'wronghost',
            'localPort' => '33306',
            'remoteHost' => 'mysql',
            'remotePort' => '3306',
        ];

        $logger = new TestLogger();

        $this->getApp($config, $logger)->execute();
        Assert::assertTrue($logger->hasInfo('Exporting "escaping" to "in.c-main.simple".'));
    }

    public function testRunWithWrongCredentials(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['db']['#password'] = 'somecrap';

        $this->expectExceptionMessage('Error connecting to DB: SQLSTATE[HY000] [1045] Access denied for user');
        $this->expectException(UserExceptionInterface::class);
        $this->getApp($config)->execute();
    }

    public function testRetries(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['tables'][0]['query'] = 'SELECT * FROM `table_that_does_not_exist`';
        $config['parameters']['tables'][0]['retries'] = 3;

        try {
            $this->getApp($config)->execute();
        } catch (UserExceptionInterface $e) {
            Assert::assertStringContainsString('Tried 3 times', $e->getMessage());
        }
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
        Assert::assertIsArray($manifest);
        Assert::assertArrayNotHasKey('schema', $manifest);
    }

    public function testTestConnection(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'testConnection';
        $config['parameters']['tables'] = [];
        $logger = new TestLogger();

        ob_start();
        $this->getApp($config, $logger)->execute();
        $res = json_decode((string) ob_get_contents(), true);
        ob_end_clean();

        Assert::assertEquals('success', $res['status']);
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
            $this->getApp($config)->execute();
            $this->fail('Failing query must raise exception.');
        } catch (UserExceptionInterface $e) {
            // test that the error message contains the query name
            Assert::assertStringContainsString('[dummy]', $e->getMessage());
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
            $app->execute();
        } catch (UserExceptionInterface $e) {
            $exceptionThrown = true;
        }

        Assert::assertTrue($exceptionThrown);
    }

    public function testGetTablesAction(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'getTables';

        $app = $this->getApp($config);

        ob_start();
        $app->execute();
        $result = json_decode((string) ob_get_contents(), true);
        ob_end_clean();

        Assert::assertArrayHasKey('status', $result);
        Assert::assertArrayHasKey('tables', $result);
        Assert::assertEquals('success', $result['status']);
        Assert::assertCount(3, $result['tables']);

        unset($result['tables'][0]['rowCount']);
        unset($result['tables'][1]['rowCount']);
        unset($result['tables'][3]['rowCount']);

        $expectedData = [
            0 =>
                [
                    'name' => 'escaping',
                    'schema' => 'testdb',
                    'columns' =>
                        [
                            [
                                'name' => 'col1',
                                'type' => 'varchar',
                                'primaryKey' => false,
                            ],
                            [
                                'name' => 'col2',
                                'type' => 'varchar',
                                'primaryKey' => false,
                            ],
                        ],
                ],
            1 =>
                [
                    'name' => 'escapingPK',
                    'schema' => 'testdb',
                    'columns' =>
                        [
                            [
                                'name' => 'col1',
                                'type' => 'varchar',
                                'primaryKey' => true,
                            ],
                            [
                                'name' => 'col2',
                                'type' => 'varchar',
                                'primaryKey' => true,
                            ],
                        ],
                ],
            2 =>
                [
                    'name' => 'simple',
                    'schema' => 'testdb',
                    'columns' =>
                        [
                            [
                                'name' => '_weird-I-d',
                                'type' => 'varchar',
                                'primaryKey' => true,
                            ],
                            [
                                'name' => 'SãoPaulo',
                                'type' => 'varchar',
                                'primaryKey' => false,
                            ],
                        ],
                ],
        ];

        Assert::assertEquals($expectedData, $result['tables']);
    }

    public function testGetTablesWithoutColumns(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'getTables';
        $config['parameters']['tableListFilter'] = [
            'listColumns' => false,
            'tablesToList' => [],
        ];

        $app = $this->getApp($config);

        ob_start();
        $app->execute();
        $result = json_decode((string) ob_get_contents(), true);
        ob_end_clean();

        Assert::assertArrayHasKey('status', $result);
        Assert::assertArrayHasKey('tables', $result);
        Assert::assertEquals('success', $result['status']);
        Assert::assertCount(3, $result['tables']);

        unset($result['tables'][0]['rowCount']);
        unset($result['tables'][1]['rowCount']);
        unset($result['tables'][3]['rowCount']);

        $expectedData = [
            [
                'name' => 'escaping',
                'schema' => 'testdb',
            ],
            [
                'name' => 'escapingPK',
                'schema' => 'testdb',
            ],
            [
                'name' => 'simple',
                'schema' => 'testdb',
            ],
        ];

        Assert::assertEquals($expectedData, $result['tables']);
    }

    public function testGetTablesWithColumnsOnlyOneTable(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'getTables';
        $config['parameters']['tableListFilter'] = [
            'listColumns' => true,
            'tablesToList' => [
                [
                    'tableName' => 'simple',
                    'schema' => 'testdb',
                ],
            ],
        ];

        $app = $this->getApp($config);

        ob_start();
        $app->execute();
        $result = json_decode((string) ob_get_contents(), true);
        ob_end_clean();

        Assert::assertArrayHasKey('status', $result);
        Assert::assertArrayHasKey('tables', $result);
        Assert::assertEquals('success', $result['status']);
        Assert::assertCount(1, $result['tables']);

        unset($result['tables'][0]['rowCount']);

        $expectedData = [
            [
                'name' => 'simple',
                'schema' => 'testdb',
                'columns' =>
                    [
                        [
                            'name' => '_weird-I-d',
                            'type' => 'varchar',
                            'primaryKey' => true,
                        ],
                        [
                            'name' => 'SãoPaulo',
                            'type' => 'varchar',
                            'primaryKey' => false,
                        ],
                    ],
            ],
        ];

        Assert::assertEquals($expectedData, $result['tables']);
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

        Assert::assertIsArray($outputManifest);
        Assert::assertArrayHasKey('destination', $outputManifest);
        Assert::assertArrayHasKey('incremental', $outputManifest);
        Assert::assertArrayHasKey('table_metadata', $outputManifest);

        $expectedMetadata = [
            'KBC.name' => 'simple',
            'KBC.schema' => 'testdb',
            'KBC.type' => 'BASE TABLE',
            'KBC.sanitizedName' => 'simple',
            'KBC.rowCount' => 2,
        ];

        assertEquals($expectedMetadata, $outputManifest['table_metadata']);

        Assert::assertArrayHasKey('schema', $outputManifest);
        Assert::assertCount(2, $outputManifest['schema']);
        Assert::assertEquals('weird_I_d', $outputManifest['schema'][0]['name']);
        Assert::assertEquals('SaoPaulo', $outputManifest['schema'][1]['name']);

        $expectedColumnMetadata = [
            [
                'nullable' => false,
                'primary_key' => true,
                'metadata' => [
                    'KBC.sourceName' => '_weird-I-d',
                    'KBC.sanitizedName' => 'weird_I_d',
                    'KBC.uniqueKey' => false,
                    'KBC.ordinalPosition' => 1,
                    'KBC.constraintName' => 'PRIMARY',
                ],
                'name' => 'weird_I_d',
                'data_type' => [
                    'base' => [
                        'default' => 'abc',
                        'length' => '155',
                        'type' => 'varchar',
                    ],
                ],
            ],
            [
                'nullable' => false,
                'primary_key' => false,
                'metadata' => [
                    'KBC.sourceName' => 'SãoPaulo',
                    'KBC.sanitizedName' => 'SaoPaulo',
                    'KBC.uniqueKey' => false,
                    'KBC.ordinalPosition' => 2,
                ],
                'name' => 'SaoPaulo',
                'data_type' => [
                    'base' => [
                        'default' => 'abc',
                        'length' => '155',
                        'type' => 'varchar',
                    ],
                ],
            ],
        ];

        Assert::assertEquals($expectedColumnMetadata, $outputManifest['schema']);
    }

    public function testNonExistingAction(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'sample';
        $config['parameters']['tables'] = [];

        $this->expectExceptionMessage('Unknown sync action "sample", method does not exist in class');
        $this->expectException(BaseComponentException::class);
        $app = $this->getApp($config);
        $app->execute();
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
        Assert::assertIsArray($manifest);
        Assert::assertEquals(['weird_I_d', 'SaoPaulo'], array_column($manifest['schema'], 'name'));
        Assert::assertTrue($manifest['schema'][0]['primary_key']);
    }

    public function testInvalidConfigurationQueryAndTable(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['tables'][0]['table'] = ['schema' => 'testdb', 'tableName' => 'escaping'];
        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Both table and query cannot be set together.');
        $this->getApp($config)->execute();
    }

    public function testInvalidConfigurationQueryNorTable(): void
    {
        $config = $this->getConfig(self::DRIVER);
        unset($config['parameters']['tables'][0]['query']);
        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Table or query must be configured.');
        $this->getApp($config)->execute();
    }

    public function testStrangeTableName(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['tables'][0]['outputTable'] = 'in.c-main.something/ weird';
        unset($config['parameters']['tables'][1]);
        $logger = new TestLogger();
        $this->getApp($config, $logger)->execute();

        Assert::assertTrue($logger->hasInfo('Exported "7" rows to "in.c-main.something/ weird".'));
        Assert::assertFileExists($this->dataDir . '/out/tables/in.c-main.something-weird.csv');
        Assert::assertFileExists($this->dataDir . '/out/tables/in.c-main.something-weird.csv.manifest');
    }

    public function testIncrementalFetchingByTimestamp(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['incrementalFethcingColumn'] = 'timestamp';
        $this->createAutoIncrementAndTimestampTable();

        $logger = new TestLogger();
        $this->getApp($config, $logger)->execute();

        Assert::assertTrue($logger->hasInfo('Exported "2" rows to "in.c-main.auto-increment-timestamp".'));

        $stateFile = $this->dataDir . '/out/state.json';

        Assert::assertFileExists($stateFile);
        $state = json_decode((string) file_get_contents($stateFile), true);

        //check that output state contains expected information
        Assert::assertArrayHasKey('lastFetchedRow', $state);
        Assert::assertNotEmpty($state['lastFetchedRow']);

        sleep(2);
        // the next fetch should return row with last fetched value
        $logger->reset();
        $this->cleanStateFiles();
        $this->getApp($config, $logger, $state)->execute();
        Assert::assertTrue($logger->hasInfo('Exported "1" rows to "in.c-main.auto-increment-timestamp".'));

        sleep(2);
        //now add a couple rows and run it again.
        $this->db->exec('INSERT INTO auto_increment_timestamp (`name`) VALUES (\'charles\'), (\'william\')');

        $logger->reset();
        $this->cleanStateFiles();
        $this->getApp($config, $logger, $state)->execute();

        $newState = json_decode((string) file_get_contents($stateFile), true);

        //check that output state contains expected information
        Assert::assertArrayHasKey('lastFetchedRow', $newState);
        Assert::assertGreaterThan(
            $state['lastFetchedRow'],
            $newState['lastFetchedRow'],
        );
    }

    public function testIncrementalFetchingByAutoIncrement(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['incrementalFethcingColumn'] = 'id';
        $this->createAutoIncrementAndTimestampTable();

        $logger = new TestLogger();
        $this->getApp($config, $logger)->execute();

        Assert::assertTrue($logger->hasInfo('Exported "2" rows to "in.c-main.auto-increment-timestamp".'));

        $stateFile = $this->dataDir . '/out/state.json';

        Assert::assertFileExists($stateFile);
        $state = json_decode((string) file_get_contents($stateFile), true);

        //check that output state contains expected information
        Assert::assertArrayHasKey('lastFetchedRow', $state);
        Assert::assertEquals(2, $state['lastFetchedRow']);

        sleep(2);
        // the next fetch should return row with last fetched value
        $logger->reset();
        $this->cleanStateFiles();
        $this->getApp($config, $logger, $state)->execute();
        Assert::assertFileExists($stateFile);
        Assert::assertTrue($logger->hasInfo('Exported "1" rows to "in.c-main.auto-increment-timestamp".'));

        sleep(2);
        //now add a couple rows and run it again.
        $this->db->exec('INSERT INTO auto_increment_timestamp (`name`) VALUES (\'charles\'), (\'william\')');

        $logger->reset();
        $this->cleanStateFiles();
        $this->getApp($config, $logger, $state)->execute();
        Assert::assertFileExists($stateFile);
        Assert::assertTrue($logger->hasInfo('Exported "3" rows to "in.c-main.auto-increment-timestamp".'));

        $state = json_decode((string) file_get_contents($stateFile), true);

        //check that output state contains expected information
        Assert::assertArrayHasKey('lastFetchedRow', $state);
        Assert::assertEquals(4, $state['lastFetchedRow']);
    }

    public function testIncrementalMaxNumberValue(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'number';
        $this->createAutoIncrementAndTimestampTable();

        $logger = new TestLogger();
        $this->getApp($config, $logger)->execute();

        Assert::assertTrue($logger->hasInfo('Exported "2" rows to "in.c-main.auto-increment-timestamp".'));

        $stateFile = $this->dataDir . '/out/state.json';

        Assert::assertFileExists($stateFile);
        $state = json_decode((string) file_get_contents($stateFile), true);

        $this->db->exec(
            'INSERT INTO auto_increment_timestamp (`name`, `number`)' .
            ' VALUES (\'charles\', 20.23486237628), (\'william\', 21.2863763287638276)',
        );

        $this->cleanStateFiles();
        $this->getApp($config, $logger)->execute();

        Assert::assertFileExists($stateFile);
        $newState = json_decode((string) file_get_contents($stateFile), true);

        Assert::assertArrayHasKey('lastFetchedRow', $newState);
        Assert::assertEquals('21.28637632876382760000', $newState['lastFetchedRow']);

        // Last fetched value is also present in the results of the next run ...
        // so 4 = 2 rows with same timestamp = last fetched value + 2 new rows
        Assert::assertTrue($logger->hasInfo('Exported "4" rows to "in.c-main.auto-increment-timestamp".'));
    }

    public function testIncrementalFetchingLimit(): void
    {
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingLimit'] = 1;
        $this->createAutoIncrementAndTimestampTable();

        $logger = new TestLogger();
        $this->getApp($config, $logger)->execute();

        Assert::assertTrue($logger->hasInfo('Exported "1" rows to "in.c-main.auto-increment-timestamp".'));

        $stateFile = $this->dataDir . '/out/state.json';

        Assert::assertFileExists($stateFile);
        $state = json_decode((string) file_get_contents($stateFile), true);

        //check that output state contains expected information
        Assert::assertArrayHasKey('lastFetchedRow', $state);
        Assert::assertEquals(1, $state['lastFetchedRow']);

        sleep(2);
        // the next fetch should contain the second row
        $logger->reset();
        $this->cleanStateFiles();
        $this->getApp($config, $logger)->execute();

        Assert::assertTrue($logger->hasInfo('Exported "1" rows to "in.c-main.auto-increment-timestamp".'));

        $newState = json_decode((string) file_get_contents($stateFile), true);

        //check that output state contains expected information
        Assert::assertArrayHasKey('lastFetchedRow', $newState);

        // Last fetched value is also present in the results of the next run ...
        // ... and LIMIT = 1   =>  returned same value as in the first run
        Assert::assertEquals(1, $newState['lastFetchedRow']);
    }

    public function testIncrementalFetchingDisabled(): void
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        unset($config['parameters']['incremental']);
        unset($config['parameters']['incrementalFetchingColumn']);

        $logger = new TestLogger();
        $this->getApp($config, $logger)->execute();

        Assert::assertTrue($logger->hasInfo('Exported "2" rows to "in.c-main.auto-increment-timestamp".'));

        $stateFile = $this->dataDir . '/out/state.json';

        Assert::assertFileDoesNotExist($stateFile);

        // Check manifest incremental key
        $outputManifest = JsonHelper::readFile(
            $this->dataDir . '/out/tables/in.c-main.auto-increment-timestamp.csv.manifest',
        );
        Assert::assertFalse($outputManifest['incremental']);
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

    public function testIncrementalFetchingInvalidColumns(): void
    {
        $this->createAutoIncrementAndTimestampTable();
        $config = $this->getIncrementalFetchingConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'fakeCol'; // column does not exist

        try {
            $this->getApp($config)->execute();
            $this->fail('specified autoIncrement column does not exist, should fail.');
        } catch (UserExceptionInterface $e) {
            Assert::assertStringStartsWith('Column [fakeCol]', $e->getMessage());
        }

        // column exists but is not auto-increment nor updating timestamp so should fail
        $config['parameters']['incrementalFetchingColumn'] = 'name';
        try {
            $this->getApp($config)->execute();
            $this->fail('specified column is not auto increment nor timestamp, should fail.');
        } catch (UserExceptionInterface $e) {
            Assert::assertStringStartsWith('Column [name] specified for incremental fetching', $e->getMessage());
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
            'The "incrementalFetchingColumn" is configured, but incremental fetching ' .
            'is not supported for custom query.',
        );
        $this->getApp($config)->execute();
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
        Assert::assertIsArray($outputManifest);

        // check that the manifest has the correct column ordering
        Assert::assertEquals($config['parameters']['columns'], array_column($outputManifest['schema'], 'name'));
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

        $logger = new TestLogger();
        ob_start();
        $this->getApp($config, $logger)->execute();
        $result = json_decode((string) ob_get_contents(), true);
        ob_end_clean();

        Assert::assertCount(1, $result);
        Assert::assertArrayHasKey('status', $result);
        Assert::assertEquals('success', $result['status']);
    }

    public function testConfigWithNoName(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['name']);
        unset($config['parameters']['table']);
        // we want to test the no results case
        $config['parameters']['query'] = 'SELECT 1 LIMIT 0';

        $logger = new TestLogger();
        $this->getApp($config, $logger)->execute();

        Assert::assertTrue($logger->hasWarning(
            'Query result set is empty. Exported "0" rows to "in.c-main.simple".',
        ));
    }

    public function testInvalidConfigsBothTableAndQueryWithNoName(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['name']);

        // we want to test the no results case
        $config['parameters']['query'] = 'SELECT 1 LIMIT 0';

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Both table and query cannot be set together.');

        $this->getApp($config)->execute();
    }

    public function testInvalidConfigsBothIncrFetchAndQueryWithNoName(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['name']);
        unset($config['parameters']['table']);
        $config['parameters']['incrementalFetchingColumn'] = 'abc';

        // we want to test the no results case
        $config['parameters']['query'] = 'SELECT 1 LIMIT 0';

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'The "incrementalFetchingColumn" is configured, but incremental fetching ' .
            'is not supported for custom query.',
        );

        ($this->getApp($config))->execute();
    }

    public function testInvalidConfigsNeitherTableNorQueryWithNoName(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['name']);
        unset($config['parameters']['table']);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Table or query must be configured.');
        $this->getApp($config)->execute();
    }

    public function testInvalidConfigsInvalidTableWithNoName(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['name']);
        $config['parameters']['table'] = ['tableName' => 'sales'];

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('The child config "schema" under "root.parameters.table" must be configured.');
        $this->getApp($config)->execute();
    }

    public function testNoRetryOnCsvError(): void
    {
        $config = $this->getConfigRowForCsvErr(self::DRIVER);

        (new Filesystem)->remove($this->dataDir . '/out/tables/in.c-main.simple-csv-err.csv');
        (new Filesystem)->symlink('/dev/full', $this->dataDir . '/out/tables/in.c-main.simple-csv-err.csv');

        $logger = new TestLogger();
        $app = $this->getApp($config, $logger);
        try {
            $app->execute();
            self::fail('Must raise exception');
        } catch (ApplicationExceptionInterface $e) {
            Assert::assertStringContainsString('Failed writing CSV File', $e->getMessage());
            Assert::assertFalse($logger->hasInfoThatContains('Retrying'));
        }
    }

    public function testSshWithCompression(): void
    {
        $this->cleanOutputDirectory();
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'user' => $config['parameters']['db']['user'],
            'keys' => [
                '#private' => $this->getPrivateKey(),
                'public' => $this->getPublicKey(),
            ],
            'sshHost' => 'sshproxy',
            'localPort' => '33056',
            'compression' => true,
        ];

        $logger = new TestLogger();
        $this->getApp($config, $logger)->execute();
        $this->assertExtractedData($this->dataDir . '/escaping.csv', 'in.c-main.escaping');
        $this->assertExtractedData($this->dataDir . '/simple.csv', 'in.c-main.simple');

        // Connecting to SSH proxy, not to database directly
        $this->assertTrue($logger->hasInfoThatContains("Creating SSH tunnel to 'sshproxy' on local port '33056'"));
        $this->assertTrue($logger->hasInfoThatContains('Creating PDO connection to "mysql:host=127.0.0.1;port=33056'));
    }

    public function testSshWithCompressionConfigRow(): void
    {
        $this->cleanOutputDirectory();
        $config = $this->getConfigRow(self::DRIVER);
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'user' => $config['parameters']['db']['user'],
            'keys' => [
                '#private' => $this->getPrivateKey(),
                'public' => $this->getPublicKey(),
            ],
            'sshHost' => 'sshproxy',
            'localPort' => '33066',
            'compression' => true,
        ];

        $logger = new TestLogger();
        $this->getApp($config, $logger)->execute();
        $this->assertExtractedData($this->dataDir . '/simple.csv', 'in.c-main.simple');

        // Connecting to SSH proxy, not to database directly
        $this->assertTrue($logger->hasInfoThatContains("Creating SSH tunnel to 'sshproxy' on local port '33066'"));
        $this->assertTrue($logger->hasInfoThatContains('Creating PDO connection to "mysql:host=127.0.0.1;port=33066'));
    }

    public function testWillRetryConnectingToServer(): void
    {
        $handler = new TestHandler();
        $logger = new Logger('test');
        $logger->pushHandler($handler);
        $config = $this->getConfigRow(self::DRIVER);
        $config['parameters']['db']['host'] = 'nonexistenthost.example';
        $app = $this->getApp($config, $logger);
        try {
            $app->execute();
            self::fail('Must raise exception.');
        } catch (UserExceptionInterface $e) {
            Assert::assertTrue($handler->hasInfoThatContains('Retrying...'));
            Assert::assertStringContainsString('Error connecting to ' .
                'DB: SQLSTATE[HY000] [2002] ' .
                'php_network_getaddresses: getaddrinfo for nonexistenthost.example ' .
                'failed: Name or service not known', $e->getMessage());
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
