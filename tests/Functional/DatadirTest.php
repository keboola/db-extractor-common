<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Functional;

use Keboola\DatadirTests\AbstractDatadirTestCase;
use Keboola\DatadirTests\DatadirTestSpecification;
use Keboola\DbExtractor\Test\DataLoader;

class DatadirTest extends AbstractDatadirTestCase
{
    /** @var DataLoader */
    private $dataLoader;

    public function setUp(): void
    {
        parent::setUp();
        $this->dataLoader = $this->getDataLoader($this->getCredentials());
        $this->dataLoader->getPdo()->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
    }

    private function createDatabase(string $database): void
    {
        $this->dataLoader->getPdo()->exec(sprintf(
            "DROP DATABASE IF EXISTS `%s`",
            $database
        ));

        $this->dataLoader->getPdo()->exec(sprintf(
            "CREATE DATABASE `%s`
            DEFAULT CHARACTER SET utf8
            DEFAULT COLLATE utf8_general_ci",
            $database
        ));
    }

    private function createTable(string $database, string $tableName): void
    {
        $this->dataLoader->getPdo()->exec(sprintf("use %s", $database));
        $this->dataLoader->getPdo()->exec(
            "CREATE TABLE {$tableName} (
            col1 VARCHAR(128) NOT NULL, 
            col2 VARCHAR(128) NOT NULL
            )"
        );
    }

    protected function getScript(): string
    {
        return $this->getTestFileDir() . '/run.php';
    }

    private function getConfig(string $testDirectory): array
    {
        $configuration = json_decode((string) file_get_contents($testDirectory . '/config.json'), true);
        $configuration['parameters']['data_dir'] = $testDirectory;
        $configuration['parameters']['extractor_class'] = 'Common';
        return$configuration;
    }

    private function getCredentials(): array
    {
        return [
            'host' => 'mysql',
            'port' => 3306,
            'user' => 'root',
            '#password' => 'somePassword',
            'database' => 'testdb',
        ];
    }

    private function getDataLoader(array $credentials): DataLoader
    {
        return new DataLoader(
            $credentials['host'],
            (string) $credentials['port'],
            '',
            $credentials['user'],
            $credentials['#password']
        );
    }

    private function runCommonTest(
        string $testDirectory,
        array $configuration,
        int $expectedReturnCode,
        ?string $expectedStdout,
        ?string $expectedStderr
    ): void
    {
        $specification = new DatadirTestSpecification(
            $testDirectory . '/source/data',
            $expectedReturnCode,
            $expectedStdout,
            $expectedStderr,
            $testDirectory . '/expected/data/out'
        );
        $tempDatadir = $this->getTempDatadir($specification);

        file_put_contents(
            $tempDatadir->getTmpFolder() . '/config.json',
            json_encode($configuration, JSON_PRETTY_PRINT)
        );
        $process = $this->runScript($tempDatadir->getTmpFolder());
        $this->assertMatchesSpecification($specification, $process, $tempDatadir->getTmpFolder());
    }

    public function testActionTestConnection(): void
    {
        $this->markTestSkipped();

        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['action'] = 'testConnection';
        $configuration['parameters']['db'] = $credentials;

        $expectedStdout = json_encode(['status' => 'success'], JSON_PRETTY_PRINT);

        $this->createDatabase($credentials['database']);

        $this->runCommonTest(
            $testDirectory,
            $configuration,
            0,
            $expectedStdout,
            null
        );
    }

    public function testActionGetTables(): void
    {
        $this->markTestSkipped();

        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['action'] = 'getTables';
        $configuration['parameters']['db'] = $credentials;

        $response = [
            'tables' => [
                [
                    'name' => 'table1',
                    'sanitizedName' => 'table1',
                    'schema' => 'testdb',
                    'type' => 'BASE TABLE',
                    'rowCount' => '0',
                    'columns' => [
                        [
                            'name' => 'col1',
                            'sanitizedName' => 'col1',
                            'type' =>  'varchar',
                            'primaryKey' => false,
                            'length' => '128',
                            'nullable' => false,
                            'default' => null,
                            'ordinalPosition' => '1',
                        ],
                        [
                            'name' => 'col2',
                            'sanitizedName' => 'col2',
                            'type' =>  'varchar',
                            'primaryKey' => false,
                            'length' => '128',
                            'nullable' => false,
                            'default' => null,
                            'ordinalPosition' => '2',
                        ],
                    ],
                ],
                [
                    'name' => 'table2',
                    'sanitizedName' => 'table2',
                    'schema' => 'testdb',
                    'type' => 'BASE TABLE',
                    'rowCount' => '0',
                    'columns' => [
                        [
                            'name' => 'col1',
                            'sanitizedName' => 'col1',
                            'type' =>  'varchar',
                            'primaryKey' => false,
                            'length' => '128',
                            'nullable' => false,
                            'default' => null,
                            'ordinalPosition' => '1',
                        ],
                        [
                            'name' => 'col2',
                            'sanitizedName' => 'col2',
                            'type' =>  'varchar',
                            'primaryKey' => false,
                            'length' => '128',
                            'nullable' => false,
                            'default' => null,
                            'ordinalPosition' => '2',
                        ],
                    ],
                ],
            ],
            'status' => 'success',
        ];
        $expectedStdout = json_encode($response, JSON_PRETTY_PRINT);

        $database = $credentials['database'];
        $this->createDatabase($database);
        $this->createTable($database, 'table1');
        $this->createTable($database, 'table2');

        $this->runCommonTest(
            $testDirectory,
            $configuration,
            0,
            $expectedStdout,
            null
        );
    }

    public function testUndefinedAction(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['action'] = 'funkyAction';
        $configuration['parameters']['db'] = $credentials;

        $database = $credentials['database'];
        $this->createDatabase($database);

        $this->runCommonTest(
            $testDirectory,
            $configuration,
            1,
            null,
            'Action \'funkyAction\' does not exist.' . PHP_EOL
        );
    }

    public function testExportTableByOldConfigWithDefinedQueryAndTables(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['tables'] = [
            [
                'id' => 1,
                'name' => 'table1',
                'outputTable' => 'table1',
                'query' => 'SELECT * FROM table1',
                'table' => [
                    'schema' => 'testdb',
                    'tableName' => 'table1',
                ],
            ],
        ];

        $database = $credentials['database'];
        $this->createDatabase($database);
        $this->createTable($database, 'table1');

        $this->runCommonTest(
            $testDirectory,
            $configuration,
            1,
            null,
            'Invalid Configuration in "table1". Both table and query cannot be set together.' . PHP_EOL
        );
    }

    public function testExportTableByOldConfigWithoutDefinedQueryAndTables(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['tables'] = [
            [
                'id' => 1,
                'name' => 'table1',
                'outputTable' => 'table1',
            ],
        ];

        $database = $credentials['database'];
        $this->createDatabase($database);
        $this->createTable($database, 'table1');

        $this->runCommonTest(
            $testDirectory,
            $configuration,
            1,
            null,
            'Invalid Configuration in "table1". One of table or query is required.' . PHP_EOL
        );
    }

    public function testExportTableByOldConfigWithoutOutputTable(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['tables'] = [
            [
                'id' => 1,
                'name' => 'table1',
                'query' => 'SELECT * FROM table1',
            ],
        ];

        $database = $credentials['database'];
        $this->createDatabase($database);
        $this->createTable($database, 'table1');

        $this->runCommonTest(
            $testDirectory,
            $configuration,
            1,
            null,
            'The child node "outputTable" at path "parameters.tables.0" must be configured.' . PHP_EOL
        );
    }

    public function testExportTableByOldConfigOnNonExistingDatabase(): void
    {
        $testDirectory = __DIR__ . '/basic-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['tables'] = [
            [
                'id' => 1,
                'name' => 'table1',
                'table' => [
                    'schema' => 'invaliddb',
                    'tableName' => 'table1',
                ],
                'outputTable' => 'table1',
            ],
        ];

        $database = $credentials['database'];
        $table = 'table1';
        $this->createDatabase($database);
        $this->createTable($database, $table);

        $this->runCommonTest(
            $testDirectory,
            $configuration,
            1,
            null,
            '[table1]: DB query failed: SQLSTATE[42S02]:'
            . ' Base table or view not found: 1146 Table \'invaliddb.table1\' doesn\'t exist' . PHP_EOL
        );
    }

    public function testExportTableByOldConfigOnNonExistingTable(): void
    {
        $testDirectory = __DIR__ . '/basic-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['tables'] = [
            [
                'id' => 1,
                'name' => 'table1',
                'table' => [
                    'schema' => 'testdb',
                    'tableName' => 'invalidTable',
                ],
                'outputTable' => 'table1',
            ],
        ];

        $database = $credentials['database'];
        $table = 'table1';
        $this->createDatabase($database);
        $this->createTable($database, $table);

        $this->runCommonTest(
            $testDirectory,
            $configuration,
            1,
            null,
            '[table1]: DB query failed: SQLSTATE[42S02]:'
            . ' Base table or view not found: 1146 Table \'testdb.invalidTable\' doesn\'t exist' . PHP_EOL
        );
    }

    public function testExportTableByOldConfigWithDefinedQuerySuccessfully(): void
    {
        $testDirectory = __DIR__ . '/basic-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['tables'] = [
            [
                'id' => 1,
                'name' => 'table1',
                'query' => 'SELECT * FROM table1',
                'outputTable' => 'table1',
            ],
        ];

        $response = [
            'status' => 'success',
            'imported' => [
                [
                    'outputTable' => 'table1',
                    'rows' => 2,
                ]
            ],
            'state' => [],
        ];

        $database = $credentials['database'];
        $table = 'table1';
        $this->createDatabase($database);
        $this->createTable($database, $table);

        $this->dataLoader->getPdo()->exec(
            sprintf("INSERT INTO %s VALUES ('%s', '%s'), ('%s', '%s')",
            $table,
            'row1',
            'r1',
            'row2',
            'r2'
        ));

        $this->runCommonTest(
            $testDirectory,
            $configuration,
            0,
            'Exporting to table1' . PHP_EOL . json_encode($response, JSON_PRETTY_PRINT),
            null
        );
    }

    public function testExportTableByOldConfigWithDefinedTableSuccessfully(): void
    {
        $testDirectory = __DIR__ . '/basic-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['tables'] = [
            [
                'id' => 1,
                'name' => 'table1',
                'table' => [
                    'schema' => 'testdb',
                    'tableName' => 'table1',
                ],
                'outputTable' => 'table1',
            ],
        ];

        $response = [
            'status' => 'success',
            'imported' => [
                [
                    'outputTable' => 'table1',
                    'rows' => 2,
                ]
            ],
            'state' => [],
        ];

        $database = $credentials['database'];
        $table = 'table1';
        $this->createDatabase($database);
        $this->createTable($database, $table);

        $this->dataLoader->getPdo()->exec(
            sprintf("INSERT INTO %s VALUES ('%s', '%s'), ('%s', '%s')",
                $table,
                'row1',
                'r1',
                'row2',
                'r2'
            ));

        $this->runCommonTest(
            $testDirectory,
            $configuration,
            0,
            'Exporting to table1' . PHP_EOL . json_encode($response, JSON_PRETTY_PRINT),
            null
        );
    }

    public function testExportTableByOldConfigWithDefinedTableAndOneColumnSuccessfully(): void
    {
        $testDirectory = __DIR__ . '/basic-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['tables'] = [
            [
                'id' => 1,
                'name' => 'table1',
                'table' => [
                    'schema' => 'testdb',
                    'tableName' => 'table1',
                ],
                'columns' => ['col1'],
                'outputTable' => 'table1',
            ],
        ];

        $response = [
            'status' => 'success',
            'imported' => [
                [
                    'outputTable' => 'table1',
                    'rows' => 2,
                ]
            ],
            'state' => [],
        ];

        $database = $credentials['database'];
        $table = 'table1';
        $this->createDatabase($database);
        $this->createTable($database, $table);

        $this->dataLoader->getPdo()->exec(
            sprintf("INSERT INTO %s VALUES ('%s', '%s'), ('%s', '%s')",
                $table,
                'row1',
                'r1',
                'row2',
                'r2'
            ));

        $this->runCommonTest(
            $testDirectory,
            $configuration,
            0,
            'Exporting to table1' . PHP_EOL . json_encode($response, JSON_PRETTY_PRINT),
            null
        );
    }
}
