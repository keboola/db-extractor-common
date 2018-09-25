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
//use PHPUnit_Framework_Error_Warning;
use Symfony\Component\Debug\ErrorHandler;

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

      //  sleep(3600);
        exec('docker network connect db-extractor-common_db_network db_tests', $output, $code);
        fwrite(STDERR, 'InitCode: ' . $code . ' Output: ' . implode(', ', $output) .  PHP_EOL);

        //exec(self::NETWORK_KILLER_EXECUTABLE . ' 2 > /dev/null &');
        // this is useful when other tests fail and leave the connection broken
        $this->waitForConnection();
        // intentionally don't call parent, we use a different PDO connection
        //$this->pdo = $this->getConnection();
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
        $pdo = new BrokenPDO($dsn, $this->dbParams['user'], $this->dbParams['#password'], $options);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        $pdo->setAttribute(PDO::ATTR_STATEMENT_CLASS, array(BrokenPDOStatement::class, array($pdo)));
        //$pdo->setAttribute(PDO::ATTR_STATEMENT_CLASS, array(BrokenPDOStatement::class));
        //$pdo->setAttribute(PDO::ATTR_TIMEOUT, 1);
        //$pdo->query('SET connect_timeout=10')->execute(); -> global
//TODO toto se sem musi vratit, ale ne vzdycky, kvuli tomu loadu dat
//       $pdo->query('SET wait_timeout=1')->execute(); //-> tohle vypada, ze je uplne nejdulezitejsi
        //$pdo->setAttribute(PDO::ATTR_TIMEOUT, 1);
  //      $pdo->query('SET interactive_timeout=2')->execute();
    //    $pdo->query('SET net_read_timeout=1')->execute();
      //  $pdo->query('SET net_retry_count=1')->execute();
        //$pdo->query('SET net_write_timeout=1')->execute();
        //$pdo->query('SET net_buffer_length=1024')->execute(); -> global
        //$pdo->query('SET max_allowed_packet=1024')->execute(); -> global
        //$pdo->query('SET max_execution_time=2')->execute();
        return $pdo;
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
        $tableExists = count($res->fetchAll()) > 0;

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

    /*
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
    */

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
                $this->pdo = null;
                $conn = $this->getConnection();
                @$conn->query('SELECT NOW();')->execute();
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

    /*
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
        exec(self::SERVER_KILLER_EXECUTABLE . ' 2 > /dev/null &');

        $result = $app->run();

        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest');
        $this->assertEquals(self::ROW_COUNT, $this->getLineCount($outputCsvFile));
    }
    */

//    public function testNetworkKillerQuery(): void
//    {
//        /* This is not an actual tests of DbExtractorCommon, rather it tests whether network interruption
//        cause the exceptions which are expected in actual retry tests. */
//
//        /* Register symfony error handler (used in production) and replace phpunit error handler. This
//        is very important to receive correct type of exception (\ErrorException), otherwise Phpunit
//        will convert the warnings to PHPUnit_Framework_Error_Warning */
//        ErrorHandler::register(null, true);
//        $conn = $this->getConnection();
//        // sleep 1 second before killing the connection, then kill it for 3 seconds
//        // `> /dev/null &` ensures that the command is run asynchronously
//        exec(self::NETWORK_KILLER_EXECUTABLE . ' 1 3 > /dev/null &');
//        // sleep a while to make sure the connection is terminated
//        sleep(2);
//        self::expectException(\ErrorException::class);
//        self::expectExceptionMessage('Warning: PDO::query(): MySQL server has gone away');
//        $conn->query('SELECT NOW();');
//    }

//    public function testNetworkKillerExecute(): void
//    {
//        /* This is not an actual tests of DbExtractorCommon, rather it tests whether network interruption
//        cause the exceptions which are expected in actual retry tests. */
//
//        /* Register symfony error handler (used in production) and replace phpunit error handler. This
//        is very important to receive correct type of exception (\ErrorException), otherwise Phpunit
//        will convert the warnings to PHPUnit_Framework_Error_Warning */
//        ErrorHandler::register(null, true);
//        $conn = $this->getConnection();
//        // sleep 1 second before killing the connection, then kill it for 3 seconds
//        // `> /dev/null &` ensures that the command is run asynchronously
//        exec(self::NETWORK_KILLER_EXECUTABLE . ' 1 3 > /dev/null &');
//        $stmt = $conn->query('SELECT NOW();');
//        // sleep a while to make sure the connection is terminated
//        sleep(2);
//        self::expectException(\ErrorException::class);
//        self::expectExceptionMessage('Warning: PDOStatement::execute(): MySQL server has gone away');
//        $stmt->execute();
//    }

    /**
     * @throws \Exception
     * @large
     */
    public function testNetworkKillerFetch(): void
    {
       // sleep(3600);
        exec(self::SERVER_KILLER_EXECUTABLE . ' 0');
        sleep(10);
        $this->waitForConnection();
        //sleep(6600);
        /* This is not an actual tests of DbExtractorCommon, rather it tests whether network interruption
        cause the exceptions which are expected in actual retry tests. */
        $temp = new Temp();
        $temp->initRunFolder();
        $sourceFileName = $temp->getTmpFolder() . '/large.csv';
        $this->setupLargeTable($sourceFileName);

        /* Register symfony error handler (used in production) and replace phpunit error handler. This
        is very important to receive correct type of exception (\ErrorException), otherwise Phpunit
        will convert the warnings to PHPUnit_Framework_Error_Warning */
        ErrorHandler::register(null, true);
        $this->pdo->setAttribute(PDO::ATTR_TIMEOUT, 120);

        $this->pdo->query('SET wait_timeout=1')->execute(); //-> tohle vypada, ze je uplne nejdulezitejsi  - ale musi to byt az za large table
        $this->pdo->query('SET interactive_timeout=1')->execute();
//       $pdo->query('SET wait_timeout=1')->execute(); //-> tohle vypada, ze je uplne nejdulezitejsi
        $this->pdo->setAttribute(PDO::ATTR_TIMEOUT, 1);
        //$this->pdo->query('SET interactive_timeout=2')->execute();
        $this->pdo->query('SET net_read_timeout=1')->execute();
        $this->pdo->query('SET net_retry_count=1')->execute();
        $this->pdo->query('SET net_write_timeout=1')->execute();


        $stmt = $this->pdo->query('SELECT * FROM sales');
        //$stmt = $conn->query('SELECT NOW();');
        $stmt->execute();
        //unset($conn);
        // sleep 1 second before killing the connection, then kill it for 3 seconds
        // `> /dev/null &` ensures that the command is run asynchronously
        //exec(self::NETWORK_KILLER_EXECUTABLE . ' 0 60 > /dev/null &');
        // sleep a while to make sure the connection is terminated
        self::expectException(\ErrorException::class);
        self::expectExceptionMessage('Warning: Empty row packet body');
        fwrite(STDERR, sprintf('[%s] Fetch started', date('Y-m-d H:i:s')) . PHP_EOL);
        $c = 0;
        while ($row = $stmt->fetch()) {
            $c++;
            if ($c % 1000 === 0) {
                fwrite(STDERR, sprintf('[%s] Fetched %s rows', date('Y-m-d H:i:s'), $c) . PHP_EOL);
            }
        }
        fwrite(STDERR, sprintf('[%s] Fetch finished, Fetched %s rows', date('Y-m-d H:i:s'), $c) . PHP_EOL);
    }


    /*
    public function testSquirrelFetch(): void
    {
        //self::markTestSkipped('Does not work');
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
    */

    /*
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
*/

    /*
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
    */
}
