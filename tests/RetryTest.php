<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Test\ExtractorTest;
use Keboola\Temp\Temp;
use PDO;

class RetryTest extends ExtractorTest
{
    public const ROW_COUNT = 1000000;

    /** @var  array */
    private $dbParams;

    /** @var  PDO */
    private $pdo;

    public function setUp(): void
    {
        // intentionally don't call parent, we use a different PDO connection
        $this->dbParams = [
            'user' => getenv('TEST_RDS_USERNAME'),
            '#password' => getenv('TEST_RDS_PASSWORD'),
            'host' => getenv('TEST_RDS_HOST'),
            'database' => 'odin4test',
            'port' => '3306',
        ];
        $dsn = sprintf(
            "mysql:host=%s;port=%s;dbname=%s;charset=utf8",
            $this->dbParams['host'],
            $this->dbParams['port'],
            $this->dbParams['database']
        );
        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::MYSQL_ATTR_LOCAL_INFILE => true,
        ];
        $this->pdo = new PDO($dsn, $this->dbParams['user'], $this->dbParams['#password'], $options);
    }

    private function setupLargeTable(string $sourceFileName): void
    {
        $res = $this->pdo->query(
            "SELECT * 
            FROM information_schema.tables
            WHERE table_schema = 'odin4test' 
                AND table_name = 'sales'
            LIMIT 1;"
        );
        $tableExists = $res->rowCount() > 0;

        // Set up the data table
        if (!$tableExists) {
            $csv = new CsvFile($sourceFileName);
            $header = ["usergender", "usercity", "usersentiment", "zipcode", "sku", "createdat", "category"];
            $csv->writeRow($header);
            for ($i = 0; $i < self::ROW_COUNT - 1; $i++) { // -1 for the header
                $csv->writeRow([uniqid('g'), "The Lakes", "1", "89124", "ZD111402", "2013-09-23 22:38:30", uniqid('c')]);
            }
            
            $createTableSql = sprintf(
                "CREATE TABLE %s.%s (%s) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;",
                'odin4test',
                'sales',
                implode(
                    ', ',
                    array_map(function ($column) {
                        return $column . ' text NULL';
                    }, $header)
                )
            );
            $this->pdo->exec($createTableSql);
            $fileName = (string) $csv;
            $query = "
                LOAD DATA LOCAL INFILE '{$fileName}'
                INTO TABLE `odin4test`.`sales`
                CHARACTER SET utf8
                FIELDS TERMINATED BY ','
                OPTIONALLY ENCLOSED BY '\"'
                ESCAPED BY ''
                IGNORE 1 LINES
            ";
            $this->pdo->exec($query);
        }
    }

    private function getRetryConfig(): array
    {
        $config = $this->getConfig('common', 'json');
        $config['parameters']['db'] = $this->dbParams;
        $config['parameters']['tables'] = [[
            'id' => 1,
            'name' => 'sales',
            'query' => 'SELECT * FROM sales',
            'outputTable' => 'in.c-main.sales',
            'incremental' => false,
            'primaryKey' => null,
            'enabled' => true,
        ]];
        return $config;
    }

    public function testRunMainRetry(): void
    {
        $config = $this->getRetryConfig();

        $temp = new Temp();
        $temp->initRunFolder();
        $sourceFileName = $temp->getTmpFolder() . 'large.csv';
        $this->setupLargeTable($sourceFileName);

        $app = $this->getApplication('ex-db-common', $config);

        //exec('php ' . __DIR__ . '/../../killerRabbit.php 1', $output, $ret);
        //var_export($output);

        // exec async
        exec('php ' . __DIR__ . '/../../killerRabbit.php 1 > /dev/null &');
        //exec('php ' . __DIR__ . '/../../killerRabbit.php 1 > NUL');
        $result = $app->run();

        var_dump($result);

        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest');
        $this->assertEquals(self::ROW_COUNT, count(file($outputCsvFile)));
    }
}
