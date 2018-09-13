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

    private function createDatabase(): void
    {
        $this->dataLoader->getPdo()->exec(sprintf(
            "DROP DATABASE IF EXISTS `%s`",
            $this->getCredentials()['database']
        ));

        $this->dataLoader->getPdo()->exec(sprintf(
            "CREATE DATABASE `%s`
            DEFAULT CHARACTER SET utf8
            DEFAULT COLLATE utf8_general_ci",
            $this->getCredentials()['database']
        ));
    }

    private function createTable(string $tableName): void
    {
        $this->dataLoader->getPdo()->exec(sprintf("use %s", $this->getCredentials()['database']));
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

    public function testActionTestConnection(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $this->createDatabase();

        $configuration = $this->getConfig($testDirectory);
        $credentials = $this->getCredentials();

        $response = ['status' => 'success'];

        $specification = new DatadirTestSpecification(
            $testDirectory . '/source/data',
            0,
            json_encode($response, JSON_PRETTY_PRINT),
            null,
            $testDirectory . '/expected/data/out'
        );
        $tempDatadir = $this->getTempDatadir($specification);

        $configuration['action'] = 'testConnection';
        $configuration['parameters']['db'] = $credentials;
        file_put_contents(
            $tempDatadir->getTmpFolder() . '/config.json',
            json_encode($configuration, JSON_PRETTY_PRINT)
        );
        $process = $this->runScript($tempDatadir->getTmpFolder());
        $this->assertMatchesSpecification($specification, $process, $tempDatadir->getTmpFolder());
    }

    public function testActionGetTables(): void
    {
        $testDirectory = __DIR__ . '/empty-data';

        $this->createDatabase();
        $this->createTable('table1');
        $this->createTable('table2');

        $configuration = $this->getConfig($testDirectory);
        $credentials = $this->getCredentials();

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

        $specification = new DatadirTestSpecification(
            $testDirectory . '/source/data',
            0,
            json_encode($response, JSON_PRETTY_PRINT),
            null,
            $testDirectory . '/expected/data/out'
        );
        $tempDatadir = $this->getTempDatadir($specification);

        $configuration['action'] = 'getTables';
        $configuration['parameters']['db'] = $credentials;
        file_put_contents(
            $tempDatadir->getTmpFolder() . '/config.json',
            json_encode($configuration, JSON_PRETTY_PRINT)
        );
        $process = $this->runScript($tempDatadir->getTmpFolder());
        $this->assertMatchesSpecification($specification, $process, $tempDatadir->getTmpFolder());
    }
}
