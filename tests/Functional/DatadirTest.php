<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Tests\Functional;

use Keboola\Component\JsonHelper;
use Keboola\DatadirTests\AbstractDatadirTestCase;
use Keboola\DbExtractorCommon\Tests\DataLoader;

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
        $configuration = JsonHelper::readFile($testDirectory . '/config.json');
        return $configuration;
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

    public function testActionTestConnection(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['action'] = 'testConnection';
        $configuration['parameters']['db'] = $credentials;

        $expectedStdout = JsonHelper::encode(['status' => 'success']);

        $this->createDatabase($credentials['database']);

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            0,
            $expectedStdout,
            null
        );
    }

    public function testActionGetTables(): void
    {
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
                    'rowCount' => 0,
                    'columns' => [
                        [
                            'name' => 'col1',
                            'sanitizedName' => 'col1',
                            'type' =>  'varchar',
                            'primaryKey' => false,
                            'length' => '128',
                            'nullable' => false,
                            'default' => null,
                            'ordinalPosition' => 1,
                        ],
                        [
                            'name' => 'col2',
                            'sanitizedName' => 'col2',
                            'type' =>  'varchar',
                            'primaryKey' => false,
                            'length' => '128',
                            'nullable' => false,
                            'default' => null,
                            'ordinalPosition' => 2,
                        ],
                    ],
                ],
                [
                    'name' => 'table2',
                    'sanitizedName' => 'table2',
                    'schema' => 'testdb',
                    'type' => 'BASE TABLE',
                    'rowCount' => 0,
                    'columns' => [
                        [
                            'name' => 'col1',
                            'sanitizedName' => 'col1',
                            'type' =>  'varchar',
                            'primaryKey' => false,
                            'length' => '128',
                            'nullable' => false,
                            'default' => null,
                            'ordinalPosition' => 1,
                        ],
                        [
                            'name' => 'col2',
                            'sanitizedName' => 'col2',
                            'type' =>  'varchar',
                            'primaryKey' => false,
                            'length' => '128',
                            'nullable' => false,
                            'default' => null,
                            'ordinalPosition' => 2,
                        ],
                    ],
                ],
            ],
            'status' => 'success',
        ];
        $expectedStdout = JsonHelper::encode($response);

        $database = $credentials['database'];
        $this->createDatabase($database);
        $this->createTable($database, 'table1');
        $this->createTable($database, 'table2');

        $this->runTestWithCustomConfiguration(
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

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            null
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

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            'Invalid configuration for path "root.parameters.tables":'
            . ' Both "table" and "query" cannot be set together.' . PHP_EOL
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

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            'Invalid configuration for path "root.parameters.tables":'
            . ' Either "table" or "query" must be defined.' . PHP_EOL
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

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            'The child node "outputTable" at path "root.parameters.tables.0" must be configured.' . PHP_EOL
        );
    }

    public function testExportTableByOldConfigOnNonExistingDatabase(): void
    {
        $testDirectory = __DIR__ . '/empty-data';
        $invalidDatabaseName = 'invalid_db';

        $credentials = $this->getCredentials();
        $credentials['database'] = $invalidDatabaseName;

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['tables'] = [
            [
                'id' => 1,
                'name' => 'table1',
                'table' => [
                    'schema' => $invalidDatabaseName,
                    'tableName' => 'table1',
                ],
                'outputTable' => 'table1',
            ],
        ];

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            '[table1]: DB query failed: Error connecting to DB: '
            . 'SQLSTATE[HY000] [1049] Unknown database \'invalid_db\' Tried 5 times.' . PHP_EOL
        );
    }

    public function testExportTableByOldConfigOnNonExistingTable(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

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

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            '[table1]: DB query failed: SQLSTATE[42S02]: Base table or view not found:'
            . ' 1146 Table \'testdb.invalidTable\' doesn\'t exist Tried 5 times.' . PHP_EOL
        );
    }

    public function testExportTableByOldConfigWithDefinedTableWithoutDataSuccessfully(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

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
                    'rows' => 0,
                ],
            ],
            'state' => [],
        ];

        $database = $credentials['database'];
        $table = 'table1';
        $this->createDatabase($database);
        $this->createTable($database, $table);

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            0,
            'Exporting to table1' . PHP_EOL . JsonHelper::encode($response),
            null
        );
    }

    public function testExportTableByOldConfigWithDefinedQuerySuccessfully(): void
    {
        $testDirectory = __DIR__ . '/basic-data-query';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;

        $response = [
            'status' => 'success',
            'imported' => [
                [
                    'outputTable' => 'table1',
                    'rows' => 2,
                ],
            ],
            'state' => [],
        ];

        $database = $credentials['database'];
        $table = 'table1';
        $this->createDatabase($database);
        $this->createTable($database, $table);

        $this->dataLoader->getPdo()->exec(sprintf(
            "INSERT INTO %s VALUES ('%s', '%s'), ('%s', '%s')",
            $table,
            'row1',
            'r1',
            'row2',
            'r2'
        ));

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            0,
            'Exporting to table1' . PHP_EOL . JsonHelper::encode($response),
            null
        );
    }

    public function testExportTableByOldConfigWithDefinedTableSuccessfully(): void
    {
        $testDirectory = __DIR__ . '/basic-data-table';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;

        $response = [
            'status' => 'success',
            'imported' => [
                [
                    'outputTable' => 'table1',
                    'rows' => 2,
                ],
            ],
            'state' => [],
        ];

        $database = $credentials['database'];
        $table = 'table1';
        $this->createDatabase($database);
        $this->createTable($database, $table);

        $this->dataLoader->getPdo()->exec(sprintf(
            "INSERT INTO %s VALUES ('%s', '%s'), ('%s', '%s')",
            $table,
            'row1',
            'r1',
            'row2',
            'r2'
        ));

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            0,
            'Exporting to table1' . PHP_EOL . JsonHelper::encode($response),
            null
        );
    }

    public function testExportTableByOldConfigWithWrongTable(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['tables'] = [
            [
                'id' => 1,
                'name' => 'table1',
                'table' => [],
                'outputTable' => 'table1',
            ],
        ];

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            'The child node "schema" at path "root.parameters.tables.0.table" must be configured.' . PHP_EOL
        );
    }

    public function testExportTableByOldConfigWithWrongSchema(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['tables'] = [
            [
                'id' => 1,
                'name' => 'table1',
                'table' => [
                    'tableName' => 'tableName',
                ],
                'outputTable' => 'table1',
            ],
        ];

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            'The child node "schema" at path "root.parameters.tables.0.table" must be configured.' . PHP_EOL
        );
    }

    public function testExportTableByOldConfigWithWrongTableName(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['tables'] = [
            [
                'id' => 1,
                'name' => 'table1',
                'table' => [
                    'schema' => 'database',
                ],
                'outputTable' => 'table1',
            ],
        ];

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            'The child node "tableName" at path "root.parameters.tables.0.table" must be configured.' . PHP_EOL
        );
    }

    public function testExportTableByOldConfigWithDefinedTableAndIncrementalFetchingSuccessfully(): void
    {
        $testDirectory = __DIR__ . '/basic-data-table';

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
                ],
            ],
            'state' => [],
        ];

        $database = $credentials['database'];
        $table = 'table1';
        $this->createDatabase($database);
        $this->createTable($database, $table);

        $this->dataLoader->getPdo()->exec(sprintf(
            "INSERT INTO %s VALUES ('%s', '%s'), ('%s', '%s')",
            $table,
            'row1',
            'r1',
            'row2',
            'r2'
        ));

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            0,
            'Exporting to table1' . PHP_EOL . JsonHelper::encode($response),
            null
        );
    }

    public function testExportTableByOldConfigWithDefinedTablesOnlyOneEnabledSuccessfully(): void
    {
        $testDirectory = __DIR__ . '/basic-data-table';

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
            [
                'id' => 2,
                'name' => 'table2',
                'table' => [
                    'schema' => 'testdb',
                    'tableName' => 'table2',
                ],
                'outputTable' => 'table2',
                'enabled' => false,
            ],
        ];

        $response = [
            'status' => 'success',
            'imported' => [
                [
                    'outputTable' => 'table1',
                    'rows' => 2,
                ],
            ],
            'state' => [],
        ];

        $database = $credentials['database'];
        $table = 'table1';
        $this->createDatabase($database);
        $this->createTable($database, $table);

        $this->dataLoader->getPdo()->exec(sprintf(
            "INSERT INTO %s VALUES ('%s', '%s'), ('%s', '%s')",
            $table,
            'row1',
            'r1',
            'row2',
            'r2'
        ));

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            0,
            'Exporting to table1' . PHP_EOL . JsonHelper::encode($response),
            null
        );
    }

    public function testExportTableByOldConfigWithDefinedTableAndOneColumnSuccessfully(): void
    {
        $testDirectory = __DIR__ . '/basic-data-one-column';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;

        $response = [
            'status' => 'success',
            'imported' => [
                [
                    'outputTable' => 'table1',
                    'rows' => 2,
                ],
            ],
            'state' => [],
        ];

        $database = $credentials['database'];
        $table = 'table1';
        $this->createDatabase($database);
        $this->createTable($database, $table);

        $this->dataLoader->getPdo()->exec(sprintf(
            "INSERT INTO %s VALUES ('%s', '%s'), ('%s', '%s')",
            $table,
            'row1',
            'r1',
            'row2',
            'r2'
        ));

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            0,
            'Exporting to table1' . PHP_EOL . JsonHelper::encode($response),
            null
        );
    }

    public function testExportTableByOldConfigWithDifferentDatabaseAndShemaThrowsError(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();
        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['tables'] = [
            [
                'id' => 1,
                'name' => 'table_1',
                'outputTable' => 'out.table',
                'table' => [
                    'schema' => 'mismatch',
                    'tableName' => 'table_name',
                ],
            ],
        ];

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            'Invalid configuration for path "root.parameters":'
            . ' Table schema and database mismatch.' . PHP_EOL
        );
    }

    /* CONFIG ROWS */
    public function testExportTableByConfigRowsWithDefinedQueryAndTables(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['name'] = 'table1';
        $configuration['parameters']['outputTable'] = 'table1';
        $configuration['parameters']['query'] = 'SELECT * FROM table1';
        $configuration['parameters']['table'] = [
            'schema' => 'testdb',
            'tableName' => 'table1',
        ];

        $database = $credentials['database'];
        $this->createDatabase($database);
        $this->createTable($database, 'table1');

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            'Invalid configuration for path "root.parameters":'
            . ' Both "table" and "query" cannot be set together.' . PHP_EOL
        );
    }

    public function testExportTableByConfigRowsWithoutDefinedQueryAndTables(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['name'] = 'table1';
        $configuration['parameters']['outputTable'] = 'table1';

        $database = $credentials['database'];
        $this->createDatabase($database);
        $this->createTable($database, 'table1');

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            'Invalid configuration for path "root.parameters":'
            . ' Either "table" or "query" must be defined.' . PHP_EOL
        );
    }

    public function testExportTableByConfigRowsWithoutOutputTable(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['name'] = 'table1';
        $configuration['parameters']['query'] = 'SELECT * FROM table1';

        $database = $credentials['database'];
        $this->createDatabase($database);
        $this->createTable($database, 'table1');

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            'The child node "outputTable" at path "root.parameters" must be configured.' . PHP_EOL
        );
    }

    public function testExportTableByConfigRowsOnNonExistingDatabase(): void
    {
        $testDirectory = __DIR__ . '/empty-data';
        $invalidDatabaseName = 'invalid_db';

        $credentials = $this->getCredentials();
        $credentials['database'] = $invalidDatabaseName;

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['name'] = 'table1';
        $configuration['parameters']['outputTable'] = 'table1';
        $configuration['parameters']['table'] = [
            'schema' => $invalidDatabaseName,
            'tableName' => 'table1',
        ];

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            '[table1]: DB query failed: Error connecting to DB:'
            . ' SQLSTATE[HY000] [1049] Unknown database \'invalid_db\' Tried 5 times.' . PHP_EOL
        );
    }

    public function testExportTableByConfigRowsOnNonExistingTable(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['name'] = 'table1';
        $configuration['parameters']['outputTable'] = 'table1';
        $configuration['parameters']['table'] = [
            'schema' => 'testdb',
            'tableName' => 'invalidTable',
        ];

        $database = $credentials['database'];
        $table = 'table1';
        $this->createDatabase($database);
        $this->createTable($database, $table);

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            '[table1]: DB query failed: SQLSTATE[42S02]: Base table or view not found:'
            . ' 1146 Table \'testdb.invalidTable\' doesn\'t exist Tried 5 times.' . PHP_EOL
        );
    }

    public function testExportTableByConfigRowsWithDefinedTableWithoutDataSuccessfully(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['name'] = 'table1';
        $configuration['parameters']['outputTable'] = 'table1';
        $configuration['parameters']['table'] = [
            'schema' => 'testdb',
            'tableName' => 'table1',
        ];

        $response = [
            'status' => 'success',
            'imported' => [
                'outputTable' => 'table1',
                'rows' => 0,
            ],
            'state' => [],
        ];

        $database = $credentials['database'];
        $table = 'table1';
        $this->createDatabase($database);
        $this->createTable($database, $table);

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            0,
            'Exporting to table1' . PHP_EOL . JsonHelper::encode($response),
            null
        );
    }

    public function testExportTableByConfigRowsWithDefinedQuerySuccessfully(): void
    {
        $testDirectory = __DIR__ . '/basic-data-query';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        unset($configuration['parameters']['tables']);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['name'] = 'table1';
        $configuration['parameters']['outputTable'] = 'table1';
        $configuration['parameters']['query'] = 'SELECT * FROM table1';

        $response = [
            'status' => 'success',
            'imported' => [
                'outputTable' => 'table1',
                'rows' => 2,
            ],
            'state' => [],
        ];

        $database = $credentials['database'];
        $table = 'table1';
        $this->createDatabase($database);
        $this->createTable($database, $table);

        $this->dataLoader->getPdo()->exec(sprintf(
            "INSERT INTO %s VALUES ('%s', '%s'), ('%s', '%s')",
            $table,
            'row1',
            'r1',
            'row2',
            'r2'
        ));

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            0,
            'Exporting to table1' . PHP_EOL . JsonHelper::encode($response),
            null
        );
    }

    public function testExportTableByConfigRowsWithDefinedTableSuccessfully(): void
    {
        $testDirectory = __DIR__ . '/basic-data-table';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        unset($configuration['parameters']['tables']);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['name'] = 'table1';
        $configuration['parameters']['outputTable'] = 'table1';
        $configuration['parameters']['table'] = [
            'schema' => 'testdb',
            'tableName' => 'table1',
        ];

        $response = [
            'status' => 'success',
            'imported' => [
                'outputTable' => 'table1',
                'rows' => 2,
            ],
            'state' => [],
        ];

        $database = $credentials['database'];
        $table = 'table1';
        $this->createDatabase($database);
        $this->createTable($database, $table);

        $this->dataLoader->getPdo()->exec(sprintf(
            "INSERT INTO %s VALUES ('%s', '%s'), ('%s', '%s')",
            $table,
            'row1',
            'r1',
            'row2',
            'r2'
        ));

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            0,
            'Exporting to table1' . PHP_EOL . JsonHelper::encode($response),
            null
        );
    }

    public function testExportTableByConfigRowsWithWrongTable(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['name'] = 'table1';
        $configuration['parameters']['outputTable'] = 'table1';
        $configuration['parameters']['table'] = [];

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            'The child node "schema" at path "root.parameters.table" must be configured.' . PHP_EOL
        );
    }

    public function testExportTableByConfigRowsWithWrongSchema(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['name'] = 'table1';
        $configuration['parameters']['outputTable'] = 'table1';
        $configuration['parameters']['table'] = [
            'tableName' => 'table',
        ];

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            'The child node "schema" at path "root.parameters.table" must be configured.' . PHP_EOL
        );
    }

    public function testExportTableByConfigRowsWithWrongTableName(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['name'] = 'table1';
        $configuration['parameters']['outputTable'] = 'table1';
        $configuration['parameters']['table'] = [
            'schema' => 'database',
        ];

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            'The child node "tableName" at path "root.parameters.table" must be configured.' . PHP_EOL
        );
    }

    public function testExportTableByConfigRowsWithDefinedTableAndIncrementalFetchingSuccessfully(): void
    {
        $testDirectory = __DIR__ . '/basic-data-table';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        unset($configuration['parameters']['tables']);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['name'] = 'table1';
        $configuration['parameters']['outputTable'] = 'table1';
        $configuration['parameters']['table'] = [
            'schema' => 'testdb',
            'tableName' => 'table1',
        ];

        $response = [
            'status' => 'success',
            'imported' => [
                'outputTable' => 'table1',
                'rows' => 2,
            ],
            'state' => [],
        ];

        $database = $credentials['database'];
        $table = 'table1';
        $this->createDatabase($database);
        $this->createTable($database, $table);

        $this->dataLoader->getPdo()->exec(sprintf(
            "INSERT INTO %s VALUES ('%s', '%s'), ('%s', '%s')",
            $table,
            'row1',
            'r1',
            'row2',
            'r2'
        ));

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            0,
            'Exporting to table1' . PHP_EOL . JsonHelper::encode($response),
            null
        );
    }

    public function testExportTableByConfigRowsWithDefinedTableAndOneColumnSuccessfully(): void
    {
        $testDirectory = __DIR__ . '/basic-data-one-column';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        unset($configuration['parameters']['tables']);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['name'] = 'table1';
        $configuration['parameters']['outputTable'] = 'table1';
        $configuration['parameters']['table'] = [
            'schema' => 'testdb',
            'tableName' => 'table1',
        ];
        $configuration['parameters']['columns'] = [
            'col1',
        ];

        $response = [
            'status' => 'success',
            'imported' => [
                'outputTable' => 'table1',
                'rows' => 2,
            ],
            'state' => [],
        ];

        $database = $credentials['database'];
        $table = 'table1';
        $this->createDatabase($database);
        $this->createTable($database, $table);

        $this->dataLoader->getPdo()->exec(sprintf(
            "INSERT INTO %s VALUES ('%s', '%s'), ('%s', '%s')",
            $table,
            'row1',
            'r1',
            'row2',
            'r2'
        ));

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            0,
            'Exporting to table1' . PHP_EOL . JsonHelper::encode($response),
            null
        );
    }

    public function testExportTableByConfigRowsWithDefinedTableIncrementalFetchingOnNonExistingColumn(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        unset($configuration['parameters']['tables']);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['name'] = 'table1';
        $configuration['parameters']['outputTable'] = 'table1';
        $configuration['parameters']['incrementalFetchingColumn'] = 'invalid_col';
        $configuration['parameters']['table'] = [
            'schema' => 'testdb',
            'tableName' => 'table1',
        ];

        $database = $credentials['database'];
        $table = 'table1';
        $this->createDatabase($database);
        $this->createTable($database, $table);

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            'Column [invalid_col] specified for incremental fetching was not found in the table' . PHP_EOL
        );
    }

    public function testExportTableByConfigRowsWithDefinedTableIncrementalFetchingOnInvalidColumn(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        unset($configuration['parameters']['tables']);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['name'] = 'table1';
        $configuration['parameters']['outputTable'] = 'table1';
        $configuration['parameters']['incrementalFetchingColumn'] = 'col1';
        $configuration['parameters']['table'] = [
            'schema' => 'testdb',
            'tableName' => 'table1',
        ];

        $database = $credentials['database'];
        $table = 'table1';
        $this->createDatabase($database);
        $this->createTable($database, $table);

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            'Column [col1] specified for incremental fetching is not an auto increment column'
            . ' or an auto update timestamp' . PHP_EOL
        );
    }

    public function testExportTableByConfigRowsWithDefinedTableIncrementalFetchingSuccessfully(): void
    {
        $testDirectory = __DIR__ . '/basic-data-table-incremental-fetch';

        $credentials = $this->getCredentials();

        $configuration = $this->getConfig($testDirectory);
        unset($configuration['parameters']['tables']);
        $configuration['parameters']['db'] = $credentials;

        $response = [
            'status' => 'success',
            'imported' => [
                'outputTable' => 'table1',
                'rows' => 2,
            ],
            'state' => [
                'lastFetchedRow' => '2',
            ],
        ];

        $database = $credentials['database'];
        $table = 'table1';
        $this->createDatabase($database);


        $this->dataLoader->getPdo()->exec(sprintf("use %s", $database));
        $this->dataLoader->getPdo()->exec(
            "CREATE TABLE {$table} (
            id INT NOT NULL AUTO_INCREMENT,
            col1 VARCHAR(128) NOT NULL, 
            col2 VARCHAR(128) NOT NULL,
            PRIMARY KEY (id)
            )"
        );


        $this->dataLoader->getPdo()->exec(sprintf(
            "INSERT INTO %s VALUES (null, '%s', '%s'), (null, '%s', '%s')",
            $table,
            'row1',
            'r1',
            'row2',
            'r2'
        ));

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            0,
            'Exporting to table1' . PHP_EOL . JsonHelper::encode($response),
            null
        );
    }

    public function testExportTableByConfigRowsWithDifferentDatabaseAndShemaThrowsError(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $credentials = $this->getCredentials();
        $configuration = $this->getConfig($testDirectory);
        $configuration['parameters']['db'] = $credentials;
        $configuration['parameters']['outputTable'] = 'out.table';
        $configuration['parameters']['table'] = [
            'schema' => 'mismatch',
            'tableName' => 'table_name',
        ];

        $this->runTestWithCustomConfiguration(
            $testDirectory,
            $configuration,
            1,
            null,
            'Invalid configuration for path "root.parameters":'
            . ' Table schema and database mismatch.' . PHP_EOL
        );
    }
}
