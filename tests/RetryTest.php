<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\DeadConnectionException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\Common;
use Keboola\DbExtractor\Test\ExtractorTest;
use Keboola\Temp\Temp;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PDO;
use PHPUnit_Framework_Error_Warning;

class RetryTest extends ExtractorTest
{
    private const ROW_COUNT = 1000000;

    private const SERVER_KILLER_EXECUTABLE =  'php ' . __DIR__ . '/killerRabbit.php';

    private const NETWORK_KILLER_EXECUTABLE =  'php ' . __DIR__ . '/killerSquirrel.php';

    /** @var  array */
    private $dbParams;

    /** @var  PDO */
    private $pdo;

    public function setUp(): void
    {
        // this is useful when other tests fail and leave the connection broken
        $this->waitForConnection();
        // intentionally don't call parent, we use a different PDO connection
        $this->pdo = $this->getConnection();
        // unlink the output file
        @unlink($this->dataDir . '/out/tables/in.c-main.sales.csv');
    }

    private function getConnection(): PDO
    {
        $this->dbParams = [
            'user' => getenv('TEST_RDS_USERNAME'),
            '#password' => getenv('TEST_RDS_PASSWORD'),
            'host' => getenv('TEST_RDS_HOST'),
            'database' => getenv('TEST_RDS_DATABASE'),
            'port' => getenv('TEST_RDS_PORT'),
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
        return new PDO($dsn, $this->dbParams['user'], $this->dbParams['#password'], $options);
    }

    private function setupLargeTable(string $sourceFileName): void
    {
        $res = $this->pdo->query(
            sprintf(
                "SELECT * 
                FROM information_schema.tables
                WHERE table_schema = '%s' 
                    AND table_name = 'sales'
                LIMIT 1;",
                getenv('TEST_RDS_DATABASE')
            )
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
                getenv('TEST_RDS_DATABASE'),
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
            $query = sprintf(
                "
                    LOAD DATA LOCAL INFILE '%s'
                    INTO TABLE `%s`.`sales`
                    CHARACTER SET utf8
                    FIELDS TERMINATED BY ','
                    OPTIONALLY ENCLOSED BY '\"'
                    ESCAPED BY ''
                    IGNORE 1 LINES
                ",
                $fileName,
                getenv('TEST_RDS_DATABASE')
            );
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
            'retries' => 10,
        ]];
        return $config;
    }

    private function getLineCount(string $fileName): int
    {
        $lineCount = 0;
        $handle = fopen($fileName, "r");
        while (fgets($handle) !== false) {
            $lineCount++;
        }
        fclose($handle);
        return $lineCount;
    }

    private function waitForConnection(): void
    {
        $retries = 0;
        echo 'Waiting for connection' . PHP_EOL;
        while (true) {
            try {
                $conn = $this->getConnection();
                $conn->query('SELECT NOW();')->execute();
                $this->pdo = $conn;
                break;
            } catch (\Throwable $e) {
                echo 'Waiting for connection ' . $e->getMessage() . PHP_EOL;
                sleep(5);
                $retries++;
                if ($retries > 10) {
                    throw new \Exception('Killer Rabbit was too successful.');
                }
            }
        }
    }

    public function testRabbit(): void
    {
        exec(self::SERVER_KILLER_EXECUTABLE . ' 0', $output, $ret);
        $output = implode(PHP_EOL, $output);
        echo $output;
        // wait for the reboot to start (otherwise waitForConnection() would pass with the old connection
        sleep(10);
        $this->waitForConnection();

        self::assertEquals(0, $ret, $output);
        self::assertContains('Rabbit of Caerbannog', $output);
        self::assertNotEmpty($this->pdo);
    }

    public function testRunMainRetry(): void
    {
        $config = $this->getRetryConfig();

        $temp = new Temp();
        $temp->initRunFolder();
        $sourceFileName = $temp->getTmpFolder() . '/large.csv';
        $this->setupLargeTable($sourceFileName);

        $app = $this->getApplication('ex-db-common', $config);

        // exec async
        exec(self::SERVER_KILLER_EXECUTABLE . ' 2 > NUL');

        $result = $app->run();

        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest');
        $this->assertEquals(self::ROW_COUNT, $this->getLineCount($outputCsvFile));
    }

    public function testSquirrel(): void
    {
        $conn = $this->getConnection();
        exec(self::NETWORK_KILLER_EXECUTABLE . ' 0 3306 > /dev/null &');
        // a little timeout to make sure the killer has already started
        sleep(1);
        try {
            $stmt = $conn->query('SELECT NOW();');
            $stmt->execute();
            // intentionally twice, because the tcpkiller is not 100% reliable
            sleep(2);
            $stmt = $conn->query('SELECT NOW();');
            $stmt->execute();
            self::fail('Must raise an exception.');
        } catch (\Throwable $e) {
            // PDO fails to throw exception, and throws a warning only
            //self::assertInstanceOf(PHPUnit_Framework_Error_Warning::class, $e, $e->getMessage());
            self::assertInstanceOf(\PDOException::class, $e, $e->getMessage());
            self::assertTrue(
                mb_stripos($e->getMessage(), 'PDO::query(): MySQL server has gone away') !== false ||
                mb_stripos($e->getMessage(), 'Error while sending QUERY packet.') !== false,
                'Error: '. $e->getMessage()
            );
        }
    }

    public function testSquirrelFetch(): void
    {
        self::markTestSkipped('Does not work');
        $conn = $this->getConnection();
        $stmt1 = $conn->query('SELECT NOW();');
        $stmt1->execute();
        $stmt2 = $conn->query('SELECT NOW();');
        $stmt2->execute();
        exec(self::NETWORK_KILLER_EXECUTABLE . ' 0 3306 > /dev/null &');
        // a little timeout to make sure the killer has already started
        sleep(1);
        try {
            var_dump($stmt1->fetchAll());
            // intentionally twice, because the tcpkiller is not 100% reliable
            sleep(2);
            var_dump($stmt2->fetchAll());
            self::fail('Must raise an exception.');
        } catch (\Throwable $e) {
            // PDO fails to throw exception, and throws a warning only
            self::assertContains('PDOStatement::execute(): MySQL server has gone away', $e->getMessage());
            self::assertInstanceOf(PHPUnit_Framework_Error_Warning::class, $e, $e->getMessage());
        }
    }

    public function testRunMainRetryNetworkError(): void
    {
        $temp = new Temp();
        $temp->initRunFolder();
        $sourceFileName = $temp->getTmpFolder() . '/large.csv';
        $this->setupLargeTable($sourceFileName);

        $handler = new TestHandler();
        $logger = new \Keboola\DbExtractor\Logger('test');
        $logger->pushHandler($handler);
        $table = [
            'id' => 1,
            'name' => 'sales',
            'query' => 'SELECT * FROM sales',
            'outputTable' => 'in.c-main.sales',
            'incremental' => false,
            'primaryKey' => [],
            'enabled' => true,
            'retries' => 10,
            'columns' => [],
        ];

        $parameters = [
            'db' => [
                'user' => getenv('TEST_RDS_USERNAME'),
                '#password' => getenv('TEST_RDS_PASSWORD'),
                'password' => getenv('TEST_RDS_PASSWORD'),
                'host' => getenv('TEST_RDS_HOST'),
                'database' => 'odin4test',
                'port' => '3306',
            ],
            'tables' => [$table],
            'data_dir' => $this->dataDir,
            'extractor_class' => 'Common',
        ];
        $extractor = new Common($parameters, [], $logger);
        // exec async
        exec(self::NETWORK_KILLER_EXECUTABLE . ' 2 3306 > /dev/null &');
        $result = $extractor->export($table);

        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['outputTable'] . '.csv';

        self::assertFileExists($outputCsvFile);
        self::assertFileExists($this->dataDir . '/out/tables/' . $result['outputTable'] . '.csv.manifest');
        self::assertEquals(self::ROW_COUNT, $this->getLineCount($outputCsvFile));
        //var_dump($handler->getRecords());
        self::assertTrue($handler->hasInfoThatContains('Retrying...'));
    }

    public function testDeadConnectionException(): void
    {
        $config = $this->getRetryConfig();
        $config['parameters']['tables'][0]['retries'] = 0;

        $app = $this->getApplication('ex-db-common', $config);

        // exec async
        exec(self::SERVER_KILLER_EXECUTABLE . ' 2 > /dev/null &');

        try {
            $app->run();
            $this->fail("Should have failed on Dead Connection");
        } catch (UserException $ue) {
            $this->assertTrue($ue->getPrevious() instanceof DeadConnectionException);
            $this->assertContains('Dead connection', $ue->getMessage());
        }
    }
}
