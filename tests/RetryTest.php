<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\Common;
use Keboola\DbExtractor\Test\ExtractorTest;
use Keboola\Temp\Temp;
use Monolog\Handler\TestHandler;
use PDO;
use Symfony\Component\Debug\ErrorHandler;

class RetryTest extends ExtractorTest
{
    private const ROW_COUNT = 1000000;

    private const SERVER_KILLER_EXECUTABLE =  'php ' . __DIR__ . '/killerRabbit.php';

    /** @var  array */
    private $dbParams;

    /** @var  PDO */
    private $pdo;

    /**
     * @var int
     */
    private $fetchCount = 0;

    /**
     * @var bool
     */
    private $killerEnabled = false;

    /**
     * @var int
     */
    private $pid;

    /**
     * @var \PDO
     */
    private $serviceConnection;

    public function setUp(): void
    {

        //sleep(3600);
        //exec('docker network connect db-extractor-common_db_network db_tests', $output, $code);
        //fwrite(STDERR, 'InitCode: ' . $code . ' Output: ' . implode(', ', $output) .  PHP_EOL);

        //exec(self::NETWORK_KILLER_EXECUTABLE . ' 2 > /dev/null &');
        // this is useful when other tests fail and leave the connection broken
        $this->waitForConnection();
        // intentionally don't call parent, we use a different PDO connection
        //$this->pdo = $this->getConnection();
        // unlink the output file
        $this->serviceConnection = $this->getConnection();
        @unlink($this->dataDir . '/out/tables/in.c-main.sales.csv');

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
        $stmt = $this->pdo->query('SELECT CONNECTION_ID() AS pid;');
        $stmt->execute();
        $this->pid = $stmt->fetch()['pid'];
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
        $pdo = new TaintedPDO($dsn, $this->dbParams['user'], $this->dbParams['#password'], $options);
        $pdo->setOnEvent([$this, 'killConnection']);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        $pdo->setAttribute(PDO::ATTR_STATEMENT_CLASS, array(TaintedPDOStatement::class, array($pdo, [$this, 'killConnection'])));
        //$pdo->setAttribute(PDO::ATTR_STATEMENT_CLASS, array(TaintedPDOStatement::class));
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

    public function killConnection($event, $stmt, $pdo)
    {
        fwrite(STDERR, sprintf('[%s] Event: %s, Killer: %s', date('Y-m-d H:i:s'), $event, var_export($this->killerEnabled, true)) . PHP_EOL);
        if ($event === 'fetch') {
            $this->fetchCount++;
        }
        if (($this->killerEnabled === 'fetch') && ($event === 'fetch') && ($this->fetchCount % 1000 === 0)) {
            fwrite(STDERR, sprintf('[%s] Killing', date('Y-m-d H:i:s')) . PHP_EOL);
            $this->doKillConnection($pdo);
        }
        if (($this->killerEnabled === 'query') && ($event === 'query')) {
            fwrite(STDERR, sprintf('[%s] Killing', date('Y-m-d H:i:s')) . PHP_EOL);
            $this->doKillConnection($pdo);
        }
        if (($this->killerEnabled === 'execute') && ($event === 'execute')) {
            fwrite(STDERR, sprintf('[%s] Killing', date('Y-m-d H:i:s')) . PHP_EOL);
            $this->doKillConnection($pdo);
        }
        if (($this->killerEnabled === 'prepare') && ($event === 'prepare')) {
            fwrite(STDERR, sprintf('[%s] Killing', date('Y-m-d H:i:s')) . PHP_EOL);
            $this->doKillConnection($pdo);
        }

        if ($this->killerEnabled === 'query') {
            sleep(2);
        }
    }

    private function doKillConnection(\PDO $pdo)
    {
        try {
            fwrite(STDERR, sprintf('[%s] Killing connection : %s', date('Y-m-d H:i:s'), $this->pid) . PHP_EOL);
            $this->serviceConnection->exec('KILL ' . $this->pid);
        } catch (\Throwable $e) {
            fwrite(STDERR, sprintf('[%s] Kill result: %s', date('Y-m-d H:i:s'), $e->getMessage()) . PHP_EOL);
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

    private function setupLargeTable(): void
    {
        $temp = new Temp();
        $temp->initRunFolder();
        $sourceFileName = $temp->getTmpFolder() . '/large.csv';
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

    public function testServerKiller(): void
    {
        /* This is not an actual tests of DbExtractorCommon, rather it tests that the script to restart
        the MySQL server works correctly. */

        // unlike in the actual tests, here, the killer can be executed synchronously.
        exec(self::SERVER_KILLER_EXECUTABLE . ' 0', $output, $ret);
        $output = implode(PHP_EOL, $output);
        // wait for the reboot to start (otherwise waitForConnection() would pass with the old connection
        sleep(10);
        $this->waitForConnection();

        self::assertContains('Rabbit of Caerbannog', $output);
        self::assertEquals(0, $ret, $output);
        self::assertNotEmpty($this->pdo);
    }

    public function testRunMainRetry(): void
    {
        $config = $this->getRetryConfig();
        $this->setupLargeTable();
        $app = $this->getApplication('ex-db-common', $config);

        // execute asynchronously
        exec(self::SERVER_KILLER_EXECUTABLE . ' 2 > /dev/null &');

        $result = $app->run();
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv';

        self::assertEquals('success', $result['status']);
        self::assertFileExists($outputCsvFile);
        self::assertFileExists($outputCsvFile . '.manifest');
        self::assertEquals(self::ROW_COUNT, $this->getLineCount($outputCsvFile));
    }

    public function testNetworkKillerQuery(): void
    {
        /* This is not an actual tests of DbExtractorCommon, rather it tests whether network interruption
        cause the exceptions which are expected in actual retry tests. */

        /* Register symfony error handler (used in production) and replace phpunit error handler. This
        is very important to receive correct type of exception (\ErrorException), otherwise Phpunit
        will convert the warnings to PHPUnit\Framework\Error\Warning */
        ErrorHandler::register(null, true);
        //$conn = $this->getConnection();
        $this->killerEnabled = 'query';
        self::expectException(\ErrorException::class);
        self::expectExceptionMessage('Warning: PDO::query(): MySQL server has gone away');
        $this->pdo->query('SELECT NOW();');
    }

    public function testNetworkKillerExecute(): void
    {
        /* This is not an actual tests of DbExtractorCommon, rather it tests whether network interruption
        cause the exceptions which are expected in actual retry tests. */

        /* Register symfony error handler (used in production) and replace phpunit error handler. This
        is very important to receive correct type of exception (\ErrorException), otherwise Phpunit
        will convert the warnings to PHPUnit\Framework\Error\Warning */
        ErrorHandler::register(null, true);

        $stmt = $this->pdo->query('SELECT NOW();');
        $this->killerEnabled = 'execute';
        self::expectException(\ErrorException::class);
        self::expectExceptionMessage('Warning: PDOStatement::execute(): MySQL server has gone away');
        $stmt->execute();
    }


    public function testNetworkKillerFetch(): void
    {
        /* This is not an actual tests of DbExtractorCommon, rather it tests whether network interruption
        cause the exceptions which are expected in actual retry tests. */
        $temp = new Temp();
        $temp->initRunFolder();
        $sourceFileName = $temp->getTmpFolder() . '/large.csv';
        $this->setupLargeTable($sourceFileName);

        /* Register symfony error handler (used in production) and replace phpunit error handler. This
        is very important to receive correct type of exception (\ErrorException), otherwise Phpunit
        will convert the warnings to PHPUnit\Framework\Error\Warning */
        ErrorHandler::register(null, true);

        $stmt = $this->pdo->query('SELECT * FROM sales LIMIT 10000');
        $stmt->execute();
        self::expectException(\ErrorException::class);
        self::expectExceptionMessage('Warning: Empty row packet body');
        $this->killerEnabled = 'fetch';
        /** @noinspection PhpStatementHasEmptyBodyInspection */
        while ($row = $stmt->fetch()) {
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
        $config = $this->getRetryConfig();
        $config['parameters']['tables'][0]['query'] = 'SELECT * FROM sales LIMIT 99';

        $extractor = new Common($config, [], $logger);
        $reflectionProperty = new \ReflectionProperty($extractor, 'db');
        $reflectionProperty->setAccessible(true);
        $reflectionProperty->setValue($extractor, $this->pdo);

        $this->killerEnabled = 'prepare';
        /* Register symfony error handler (used in production) and replace phpunit error handler. This
        is very important to receive correct type of exception (\ErrorException), otherwise Phpunit
        will convert the warnings to PHPUnit\Framework\Error\Warning */
        ErrorHandler::register(null, true);
        $result = $extractor->export($config['parameters']['tables'][0]);
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['outputTable'] . '.csv';

        self::assertFileExists($outputCsvFile);
        self::assertFileExists($this->dataDir . '/out/tables/' . $result['outputTable'] . '.csv.manifest');
        self::assertEquals(100, $this->getLineCount($outputCsvFile));
        self::assertTrue($handler->hasInfoThatContains('Retrying...'));
        self::assertTrue($handler->hasInfoThatContains(
            'Warning: PDOStatement::execute(): MySQL server has gone away. Retrying'
        ));
    }

    public function testRunMainRetryNetworkErrorExecute(): void
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
            'query' => 'SELECT * FROM sales LIMIT 99',
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
        /*
        $extractor = self::getMockBuilder(Common::class)
            ->setMethods(['createConnection'])
            ->setConstructorArgs([$parameters, [], $logger])
            ->disableArgumentCloning()
            ->disableOriginalClone()
            ->disableAutoReturnValueGeneration()
            ->getMock();
        //$extractor->method('createConnection')->willReturnReference($this->pdo);

        $extractor->expects(self::any())->method('createConnection')
            ->with(self::anything())
            ->willReturnCallback(function (array $params) {
            //var_export($params);
            fwrite(STDERR, 'here' . get_class($this->pdo) . PHP_EOL);
            return $this->pdo;
        });
        -> nejde, protze $this->db se nastavuje uz ctoru, ale mock method az pozdeji
*/
        $extractor = new Common($parameters, [], $logger);
        $refl = new \ReflectionProperty($extractor, 'db');
        $refl->setAccessible(true);
        $refl->setValue($extractor, $this->pdo);
        ///** @var Common $extractor */
        //$mm = $extractor->createConnection([]);
        //fwrite(STDERR, 'whatabouthere' . get_class($mm) . PHP_EOL);

        // exec async
        //exec(self::NETWORK_KILLER_EXECUTABLE . ' 2 3306 > /dev/null &');
        $this->killerEnabled = 'execute';
//        /* Register symfony error handler (used in production) and replace phpunit error handler. This
//        is very important to receive correct type of exception (\ErrorException), otherwise Phpunit
//        will convert the warnings to PHPUnit\Framework\Error\Warning */
        ErrorHandler::register(null, true);
        $result = $extractor->export($table);
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['outputTable'] . '.csv';

        self::assertFileExists($outputCsvFile);
        self::assertFileExists($this->dataDir . '/out/tables/' . $result['outputTable'] . '.csv.manifest');
        self::assertEquals(100, $this->getLineCount($outputCsvFile));
        //var_dump($handler->getRecords());
        self::assertTrue($handler->hasInfoThatContains('Retrying...'));
        self::assertTrue($handler->hasInfoThatContains('Warning: PDOStatement::execute(): MySQL server has gone away. Retrying'));
    }

    public function testRunMainRetryNetworkErrorFetch(): void
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
            'query' => 'SELECT * FROM sales LIMIT 99999',
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
        /*
        $extractor = self::getMockBuilder(Common::class)
            ->setMethods(['createConnection'])
            ->setConstructorArgs([$parameters, [], $logger])
            ->disableArgumentCloning()
            ->disableOriginalClone()
            ->disableAutoReturnValueGeneration()
            ->getMock();
        //$extractor->method('createConnection')->willReturnReference($this->pdo);

        $extractor->expects(self::any())->method('createConnection')
            ->with(self::anything())
            ->willReturnCallback(function (array $params) {
            //var_export($params);
            fwrite(STDERR, 'here' . get_class($this->pdo) . PHP_EOL);
            return $this->pdo;
        });
        -> nejde, protze $this->db se nastavuje uz ctoru, ale mock method az pozdeji
*/
        $extractor = new Common($parameters, [], $logger);
        $refl = new \ReflectionProperty($extractor, 'db');
        $refl->setAccessible(true);
        $refl->setValue($extractor, $this->pdo);
        ///** @var Common $extractor */
        //$mm = $extractor->createConnection([]);
        //fwrite(STDERR, 'whatabouthere' . get_class($mm) . PHP_EOL);

        // exec async
        //exec(self::NETWORK_KILLER_EXECUTABLE . ' 2 3306 > /dev/null &');
        $this->killerEnabled = 'fetch';
//        /* Register symfony error handler (used in production) and replace phpunit error handler. This
//        is very important to receive correct type of exception (\ErrorException), otherwise Phpunit
//        will convert the warnings to PHPUnit\Framework\Error\Warning */
        ErrorHandler::register(null, true);
        $result = $extractor->export($table);
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['outputTable'] . '.csv';

        self::assertFileExists($outputCsvFile);
        self::assertFileExists($this->dataDir . '/out/tables/' . $result['outputTable'] . '.csv.manifest');
        self::assertEquals(100000, $this->getLineCount($outputCsvFile));
        //var_dump($handler->getRecords());
        self::assertTrue($handler->hasInfoThatContains('Retrying...'));
        self::assertTrue($handler->hasInfoThatContains('Warning: Empty row packet body. Retrying... [1x]'));
    }

    public function testRunMainRetryNetworkErrorFetchFAiluer(): void
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
            'query' => 'SELECT * FROM sales LIMIT 99',
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

        $extractor = self::getMockBuilder(Common::class)
            ->setMethods(['createConnection'])
            ->setConstructorArgs([$parameters, [], $logger])
            ->disableArgumentCloning()
            ->disableOriginalClone()
            ->disableAutoReturnValueGeneration()
            ->getMock();
        //$extractor->method('createConnection')->willReturnReference($this->pdo);

        $extractor->expects(self::any())->method('createConnection')
            ->with(self::anything())
            ->willReturnCallback(function (array $params) {
            //var_export($params);
            fwrite(STDERR, 'here' . get_class($this->pdo) . PHP_EOL);
            return $this->pdo;
        });
       // -> nejde, protze $this->db se nastavuje uz ctoru, ale mock method az pozdeji
//*/
  //      $extractor = new Common($parameters, [], $logger);
        $refl = new \ReflectionProperty($extractor, 'db');
        $refl->setAccessible(true);
        $refl->setValue($extractor, $this->pdo);
        ///** @var Common $extractor */
        //$mm = $extractor->createConnection([]);
        //fwrite(STDERR, 'whatabouthere' . get_class($mm) . PHP_EOL);

        // exec async
        //exec(self::NETWORK_KILLER_EXECUTABLE . ' 2 3306 > /dev/null &');
        $this->killerEnabled = 'prepare';
//        /* Register symfony error handler (used in production) and replace phpunit error handler. This
//        is very important to receive correct type of exception (\ErrorException), otherwise Phpunit
//        will convert the warnings to PHPUnit\Framework\Error\Warning */
        ErrorHandler::register(null, true);
        try {
            $result = $extractor->export($table);
            self::fail('must raise exception');
        } catch (UserException $e) {
            self::assertTrue($handler->hasInfoThatContains('Retrying...'));
            self::assertTrue($handler->hasInfoThatContains('SQLSTATE[HY000]: General error: 2006 MySQL server has gone away. Retrying... [9x]'));
            self::assertContains('[in.c-main.sales]: DB query failed: SQLSTATE[HY000]: General error: 2006 MySQL server has gone away Tried 10 times.', $e->getMessage());
        }

    }

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
