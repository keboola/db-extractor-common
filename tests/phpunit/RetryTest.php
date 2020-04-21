<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Csv\CsvWriter;
use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\Common;
use Keboola\DbExtractor\Test\ExtractorTest;
use Monolog\Logger;
use Keboola\Temp\Temp;
use Monolog\Handler\TestHandler;
use PDO;
use PHPUnit\Framework\Assert;
use Symfony\Component\Debug\ErrorHandler;

class RetryTest extends ExtractorTest
{
    private const ROW_COUNT = 1000000;

    private const SERVER_KILLER_EXECUTABLE =  'php ' . __DIR__ . '/killerRabbit.php';

    private array $dbParams;

    private PDO $taintedPdo;

    private int $fetchCount = 0;

    private ?string $killerEnabled = null;

    private int $pid;

    private PDO $serviceConnection;

    public function setUp(): void
    {
        // intentionally don't call parent, we use a different PDO connection here
        // must use waitForConnection, because the tests might leave the connection or server broken
        $this->waitForConnection();
        /* backup connection for killing the working connection to avoid error
            `SQLSTATE[HY000]: General error: 2014 Cannot execute queries while other unbuffered queries are active`. */
        $this->serviceConnection = $this->getConnection();
        // unlink the output file if any
        @unlink($this->dataDir . '/out/tables/in.c-main.sales.csv');
    }

    private function waitForConnection(): void
    {
        $retries = 0;
        while (true) {
            try {
                $conn = $this->getConnection();
                $conn->prepare('SELECT NOW();')->execute();
                $this->taintedPdo = $conn;
                break;
            } catch (\Throwable $e) {
                fwrite(STDERR, 'Waiting for connection ' . $e->getMessage() . PHP_EOL);
                sleep(5);
                $retries++;
                if ($retries > 10) {
                    throw new \Exception('Cannot establish connection to RDS.');
                }
            }
        }
        // save the PID of the current connection
        $stmt = $this->taintedPdo->prepare('SELECT CONNECTION_ID() AS pid;');
        $stmt->execute();
        $this->pid = (int) $stmt->fetch()['pid'];
    }

    private function waitForDeadConnection(): void
    {
        // wait for the connection to die
        $cnt = 0;
        while (true) {
            try {
                $this->taintedPdo->prepare('SELECT NOW();')->execute();
                $cnt++;
                if ($cnt > 50) {
                    Assert::fail('Failed to kill the server');
                }
                sleep(1);
            } catch (\Throwable $e) {
                break;
            }
        }
    }

    private function getConnection(): PDO
    {
        $this->dbParams = [
            'user' => (string) getenv('TEST_RDS_USERNAME'),
            '#password' => (string) getenv('TEST_RDS_PASSWORD'),
            'host' => (string) getenv('TEST_RDS_HOST'),
            'database' => (string) getenv('TEST_RDS_DATABASE'),
            'port' => (string) getenv('TEST_RDS_PORT'),
        ];
        $dsn = sprintf(
            'mysql:host=%s;port=%s;dbname=%s;charset=utf8',
            $this->dbParams['host'],
            $this->dbParams['port'],
            $this->dbParams['database']
        );
        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::MYSQL_ATTR_LOCAL_INFILE => true,
        ];
        $pdo = new TaintedPDO($dsn, $this->dbParams['user'], $this->dbParams['#password'], $options);
        // set a callback to the PDO class
        $pdo->setOnEvent([$this, 'killConnection']);
        $pdo->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        // replace PDOStatement with our class, pass the connection and callback in constructor
        $pdo->setAttribute(
            PDO::ATTR_STATEMENT_CLASS,
            /* value of ATTR_STATEMENT_CLASS is [className, ctorArgs], ctorArgs is array of arguments,
                there is a single argument `[$this, 'killConnection']` which is a callable */
            [TaintedPDOStatement::class, [[$this, 'killConnection']]]
        );
        return $pdo;
    }

    public function killConnection(string $event): void
    {
        // method must be public, because it's called from the TaintedPDO class
        /*
        fwrite(
            STDERR,
            sprintf(
                '[%s] Event: %s, Killer: %s',
                date('Y-m-d H:i:s'),
                $event,
                var_export($this->killerEnabled, true)
            ) . PHP_EOL
        );
        */
        if ($event === 'fetch') {
            $this->fetchCount++;
        }
        if ($this->killerEnabled && ($this->killerEnabled === $event)) {
            // kill only on every 1000th fetch event, otherwise this floods the server with errors (PID doesn't exist)
            if (($event !== 'fetch') || (($event === 'fetch') && ($this->fetchCount % 1000 === 0))) {
                //fwrite(STDERR, sprintf('[%s] Killing', date('Y-m-d H:i:s')) . PHP_EOL);
                $this->doKillConnection();
            }
        }
    }

    private function doKillConnection(): void
    {
        try {
            $this->serviceConnection->exec('KILL ' . $this->pid);
        } catch (\Throwable $e) {
            fwrite(STDERR, sprintf('[%s] Kill failed: %s', date('Y-m-d H:i:s'), $e->getMessage()) . PHP_EOL);
        }
    }

    private function getRetryConfig(): array
    {
        $config = $this->getConfig('common');
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
        $tableName = 'sales';
        $temp = new Temp();
        $sourceFileName = $temp->getTmpFolder() . '/large.csv';

        $res = $this->serviceConnection->query(sprintf(
            "SELECT * 
            FROM information_schema.tables
            WHERE table_schema = '%s' 
                AND table_name = '%s'
            LIMIT 1;",
            getenv('TEST_RDS_DATABASE'),
            $tableName
        ));

        $tableExists = $res === false ? false : count((array) $res->fetchAll()) > 0;

        // Set up the data table
        if (!$tableExists) {
            $csv = new CsvWriter($sourceFileName);
            $header = [
                'usergender',
                'usercity',
                'usersentiment',
                'zipcode',
                'sku',
                'createdat',
                'category',
            ];
            $csv->writeRow($header);
            for ($i = 0; $i < self::ROW_COUNT - 1; $i++) { // -1 for the header
                $csv->writeRow(
                    [uniqid('g'), 'The Lakes', '1', '89124', 'ZD111402', '2013-09-23 22:38:30', uniqid('c')]
                );
            }

            $createTableSql = sprintf(
                'CREATE TABLE %s.%s (%s) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;',
                getenv('TEST_RDS_DATABASE'),
                $tableName,
                implode(
                    ', ',
                    array_map(function ($column) {
                        return $column . ' text NULL';
                    }, $header)
                )
            );
            $this->serviceConnection->exec($createTableSql);
            $query = sprintf(
                "LOAD DATA LOCAL INFILE '%s'
                INTO TABLE `%s`.`%s`
                CHARACTER SET utf8
                FIELDS TERMINATED BY ','
                OPTIONALLY ENCLOSED BY '\"'
                ESCAPED BY ''
                IGNORE 1 LINES",
                $sourceFileName,
                getenv('TEST_RDS_DATABASE'),
                $tableName
            );
            $this->serviceConnection->exec($query);
        }

        $temp->remove();
    }

    private function getLineCount(string $fileName): int
    {
        $lineCount = 0;
        $handle = fopen($fileName, 'r');
        if ($handle) {
            while (fgets($handle) !== false) {
                $lineCount++;
            }
            fclose($handle);
        }
        return $lineCount;
    }

    public function testServerKiller(): void
    {
        /* This is not an actual test of DbExtractorCommon, rather it tests that the script to restart
            the MySQL server works correctly. */

        // unlike in the actual tests, here, the killer can be executed synchronously.
        exec(self::SERVER_KILLER_EXECUTABLE . ' 0', $output, $ret);
        $output = implode(PHP_EOL, $output);
        // wait for the reboot to start (otherwise waitForConnection() would pass with the old connection
        $this->waitForDeadConnection();
        try {
            $this->taintedPdo->prepare('SELECT NOW();')->execute();
            Assert::fail('Connection must be dead now.');
        } catch (\Throwable $e) {
        }
        $this->waitForConnection();

        Assert::assertStringContainsString('Rabbit of Caerbannog', $output);
        Assert::assertEquals(0, $ret, $output);
        Assert::assertNotEmpty($this->taintedPdo);
    }

    public function testNetworkKillerQuery(): void
    {
        /* This is not an actual test of DbExtractorCommon, rather it tests whether network interruption
        cause the exceptions which are expected in actual retry tests. */

        /* Register symfony error handler (used in production) and replace phpunit error handler. This
            is very important to receive correct type of exception (\ErrorException), otherwise Phpunit
            will convert the warnings to PHPUnit\Framework\Error\Warning */
        ErrorHandler::register(null, true);
        $this->killerEnabled = 'query';
        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('MySQL server has gone away');
        $this->taintedPdo->query('SELECT NOW();');
    }

    public function testNetworkKillerPrepare(): void
    {
        /* This is not an actual test of DbExtractorCommon, rather it tests whether network interruption
            cause the exceptions which are expected in actual retry tests. */

        /* Register symfony error handler (used in production) and replace phpunit error handler. This
            is very important to receive correct type of exception (\ErrorException), otherwise Phpunit
            will convert the warnings to PHPUnit\Framework\Error\Warning */
        ErrorHandler::register(null, true);

        $this->killerEnabled = 'prepare';
        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('MySQL server has gone away');
        $stmt = $this->taintedPdo->prepare('SELECT NOW();');
        $stmt->execute();
    }

    public function testNetworkKillerTruePrepare(): void
    {
        /* This is not an actual test of DbExtractorCommon, rather it tests whether network interruption
            cause the exceptions which are expected in actual retry tests. */

        /* Register symfony error handler (used in production) and replace phpunit error handler. This
            is very important to receive correct type of exception (\ErrorException), otherwise Phpunit
            will convert the warnings to PHPUnit\Framework\Error\Warning */
        ErrorHandler::register(null, true);
        $this->taintedPdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
        $this->killerEnabled = 'prepare';
        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('MySQL server has gone away');
        $this->taintedPdo->prepare('SELECT NOW();');
    }

    public function testNetworkKillerExecute(): void
    {
        /* This is not an actual test of DbExtractorCommon, rather it tests whether network interruption
            cause the exceptions which are expected in actual retry tests. */

        /* Register symfony error handler (used in production) and replace phpunit error handler. This
            is very important to receive correct type of exception (\ErrorException), otherwise Phpunit
            will convert the warnings to PHPUnit\Framework\Error\Warning */
        ErrorHandler::register(null, true);

        $stmt = $this->taintedPdo->prepare('SELECT NOW();');
        $this->killerEnabled = 'execute';
        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('MySQL server has gone away');
        $stmt->execute();
    }

    public function testNetworkKillerFetch(): void
    {
        /* This is not an actual test of DbExtractorCommon, rather it tests whether network interruption
            cause the exceptions which are expected in actual retry tests. */
        $this->setupLargeTable();

        /* Register symfony error handler (used in production) and replace phpunit error handler. This
            is very important to receive correct type of exception (\ErrorException), otherwise Phpunit
            will convert the warnings to PHPUnit\Framework\Error\Warning */
        ErrorHandler::register(null, true);

        $stmt = $this->taintedPdo->prepare('SELECT * FROM sales LIMIT 100000');
        $stmt->execute();
        $this->expectException(\ErrorException::class);
        $this->expectExceptionMessage('Warning: Empty row packet body');
        $this->killerEnabled = 'fetch';
        /** @noinspection PhpStatementHasEmptyBodyInspection */
        while ($row = $stmt->fetch()) {
        }
    }

    public function testRunMainRetryServerError(): void
    {
        /* Test that the entire table is downloaded and the result is full table (i.e. the download
            was retried, and the partial result was discarded. */
        $this->setupLargeTable();
        $config = $this->getRetryConfig();
        $app = $this->getApplication('ex-db-common', $config);

        // execute asynchronously the script to reboot the server
        exec(self::SERVER_KILLER_EXECUTABLE . ' 2 > /dev/null &');

        $result = $app->run();
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv';

        Assert::assertEquals('success', $result['status']);
        Assert::assertFileExists($outputCsvFile);
        Assert::assertFileExists($outputCsvFile . '.manifest');
        Assert::assertEquals(self::ROW_COUNT + 1, $this->getLineCount($outputCsvFile));
    }

    public function testConnectServerError(): void
    {
        self::markTestSkipped('This test is a bit unstable');
        $handler = new TestHandler();
        $logger = new Logger('test');
        $logger->pushHandler($handler);
        $this->setupLargeTable();
        $config = $this->getRetryConfig();
        // execute asynchronously the script to reboot the server
        exec(self::SERVER_KILLER_EXECUTABLE . ' 0');
        $this->waitForDeadConnection();
        $app = new Application($config, $logger, []);
        try {
            $app->run();
            Assert::fail('Must raise exception.');
        } catch (UserException $e) {
            Assert::assertFalse($handler->hasInfoThatContains('Retrying...'));
            Assert::assertStringContainsString(
                'Error connecting to DB: SQLSTATE[HY000] [2002] Connection refused',
                $e->getMessage()
            );
        }
    }

    public function testRetryNetworkErrorPrepare(): void
    {
        $this->markTestSkipped('unstable');
        $rowCount = 100;
        $this->setupLargeTable();
        $handler = new TestHandler();
        $logger = new Logger('test');
        $logger->pushHandler($handler);
        $config = $this->getRetryConfig();
        $config['parameters']['tables'][0]['query'] = 'SELECT * FROM sales LIMIT ' . $rowCount;

        $extractor = new Common($config['parameters'], [], $logger);
        // plant the tainted PDO into the extractor
        $reflectionProperty = new \ReflectionProperty($extractor, 'db');
        $reflectionProperty->setAccessible(true);
        $reflectionProperty->setValue($extractor, $this->taintedPdo);

        /* Register symfony error handler (used in production) and replace phpunit error handler. This
            is very important to receive correct type of exception (\ErrorException), otherwise Phpunit
            will convert the warnings to PHPUnit\Framework\Error\Warning */
        ErrorHandler::register(null, true);
        // This will cause interrupt before PDO::prepare(), see testNetworkKillerPrepare
        $this->killerEnabled = 'prepare';
        $result = $extractor->export($config['parameters']['tables'][0]);
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['outputTable'] . '.csv';

        Assert::assertFileExists($outputCsvFile);
        Assert::assertFileExists($outputCsvFile . '.manifest');
        Assert::assertEquals($rowCount + 1, $this->getLineCount($outputCsvFile));
        Assert::assertTrue($handler->hasInfoThatContains('Retrying...'));
        /* it's ok that `PDOStatement::execute()` is reported here,
            because prepared statements are PDO emulated (see testNetworkKillerPrepare). */
        Assert::assertTrue($handler->hasInfoThatContains(
            'Warning: PDOStatement::execute(): MySQL server has gone away. Retrying'
        ));
    }

    public function testRetryNetworkErrorExecute(): void
    {
        $rowCount = 100;
        $this->setupLargeTable();
        $handler = new TestHandler();
        $logger = new Logger('test');
        $logger->pushHandler($handler);
        $config = $this->getRetryConfig();
        $config['parameters']['tables'][0]['query'] = 'SELECT * FROM sales LIMIT ' . $rowCount;

        $extractor = new Common($config['parameters'], [], $logger);
        // plant the tainted PDO into the extractor
        $reflectionProperty = new \ReflectionProperty($extractor, 'db');
        $reflectionProperty->setAccessible(true);
        $reflectionProperty->setValue($extractor, $this->taintedPdo);

        /* Register symfony error handler (used in production) and replace phpunit error handler. This
            is very important to receive correct type of exception (\ErrorException), otherwise Phpunit
            will convert the warnings to PHPUnit\Framework\Error\Warning */
        ErrorHandler::register(null, true);
        // This will cause interrupt before PDO::prepare(), see testNetworkKillerPrepare
        $this->killerEnabled = 'prepare';
        $result = $extractor->export($config['parameters']['tables'][0]);
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['outputTable'] . '.csv';

        Assert::assertFileExists($outputCsvFile);
        Assert::assertFileExists($outputCsvFile . '.manifest');
        Assert::assertEquals(100 + 1, $this->getLineCount($outputCsvFile));
        Assert::assertTrue($handler->hasInfoThatContains('Retrying...'));
        Assert::assertTrue($handler->hasInfoThatContains(
            'MySQL server has gone away. Retrying'
        ));
    }

    public function testRetryNetworkErrorFetch(): void
    {
        /* This has to be large enough so that it doesn't fit into
            prefetch cache (which exists, but seems to be undocumented). */
        $rowCount = 1000000;
        $this->setupLargeTable();
        $handler = new TestHandler();
        $logger = new Logger('test');
        $logger->pushHandler($handler);
        $config = $this->getRetryConfig();
        $config['parameters']['tables'][0]['query'] = 'SELECT * FROM sales LIMIT ' . $rowCount;

        $extractor = new Common($config['parameters'], [], $logger);
        // plant the tainted PDO into the extractor
        $reflectionProperty = new \ReflectionProperty($extractor, 'db');
        $reflectionProperty->setAccessible(true);
        $reflectionProperty->setValue($extractor, $this->taintedPdo);

        /* Register symfony error handler (used in production) and replace phpunit error handler. This
            is very important to receive correct type of exception (\ErrorException), otherwise Phpunit
            will convert the warnings to PHPUnit\Framework\Error\Warning */
        ErrorHandler::register(null, true);
        // This will cause interrupt before PDO::prepare(), see testNetworkKillerPrepare
        $this->killerEnabled = 'fetch';
        $result = $extractor->export($config['parameters']['tables'][0]);
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['outputTable'] . '.csv';

        Assert::assertFileExists($outputCsvFile);
        Assert::assertFileExists($outputCsvFile . '.manifest');
        Assert::assertEquals($rowCount + 1, $this->getLineCount($outputCsvFile));
        Assert::assertTrue($handler->hasInfoThatContains('Retrying...'));
        Assert::assertTrue($handler->hasInfoThatContains('Warning: Empty row packet body. Retrying... [1x]'));
    }

    public function testRetryNetworkErrorFetchFailure(): void
    {
        $rowCount = 100;
        $this->setupLargeTable();
        $handler = new TestHandler();
        $logger = new Logger('test');
        $logger->pushHandler($handler);
        $config = $this->getRetryConfig();
        $config['parameters']['tables'][0]['query'] = 'SELECT * FROM sales LIMIT ' . $rowCount;

        // plant the tainted PDO into the createConnection method of the extractor
        $extractor = self::getMockBuilder(Common::class)
            ->setMethods(['createConnection'])
            ->setConstructorArgs([$config['parameters'], [], $logger])
            ->disableAutoReturnValueGeneration()
            ->getMock();
        $extractor->method('createConnection')->willReturn($this->taintedPdo);

        // plant the tainted PDO into the extractor
        $reflectionProperty = new \ReflectionProperty($extractor, 'db');
        $reflectionProperty->setAccessible(true);
        $reflectionProperty->setValue($extractor, $this->taintedPdo);

        /* Both of the above are needed because the DB connection is initialized in the Common::__construct()
            using the `createConnection` method. That means that during instantiation of the mock, the createConnection
            method is called and returns garbage (because it's a mock with yet undefined value). The original
            constructor cannot be disabled, because it does other things (and even if it weren't, it still has to
            set the connection internally. That's why we need to use reflection to plant our PDO into the extractor.
            (so that overwrites the mock garbage which gets there in the ctor).
            However, when the connection is broken, and retried, the `createConnection` method is called again, by
            making it return the tainted PDO too, we'll ensure that the extractor will be unable to recreate the
            connection, thus testing that it does fail after certain number of retries.
        */

        /* Register symfony error handler (used in production) and replace phpunit error handler. This
            is very important to receive correct type of exception (\ErrorException), otherwise Phpunit
            will convert the warnings to PHPUnit\Framework\Error\Warning */
        ErrorHandler::register(null, true);
        $this->killerEnabled = 'prepare';
        try {
            /** @var Common $extractor */
            $extractor->export($config['parameters']['tables'][0]);
            Assert::fail('Must raise exception.');
        } catch (UserException $e) {
            Assert::assertTrue($handler->hasInfoThatContains('Retrying...'));
            Assert::assertTrue($handler->hasInfoThatContains(
                'SQLSTATE[HY000]: General error: 2006 MySQL server has gone away. Retrying... [9x]'
            ));
            Assert::assertStringContainsString(
                'query failed: SQLSTATE[HY000]: General error: 2006 MySQL server has gone away Tried 10 times.',
                $e->getMessage()
            );
        }
    }

    public function testRetryException(): void
    {
        $this->markTestSkipped('unstable');
        $handler = new TestHandler();
        $logger = new Logger('test');
        $logger->pushHandler($handler);
        $config = $this->getRetryConfig();
        $config['parameters']['tables'][0]['query'] = 'SELECT * FROM non_existent_table';

        $extractor = new Common($config['parameters'], [], $logger);
        // plant the tainted PDO into the extractor
        $reflectionProperty = new \ReflectionProperty($extractor, 'db');
        $reflectionProperty->setAccessible(true);
        $reflectionProperty->setValue($extractor, $this->taintedPdo);

        /* Register symfony error handler (used in production) and replace phpunit error handler. This
            is very important to receive correct type of exception (\ErrorException), otherwise Phpunit
            will convert the warnings to PHPUnit\Framework\Error\Warning */
        ErrorHandler::register(null, true);
        try {
            $extractor->export($config['parameters']['tables'][0]);
            Assert::fail('Must raise exception.');
        } catch (UserException $e) {
            Assert::assertStringContainsString(
                'non_existent_table\' doesn\'t exist Tried 10 times',
                $e->getMessage()
            );
            Assert::assertTrue($handler->hasInfoThatContains('Retrying...'));
            Assert::assertTrue($handler->hasInfoThatContains(
                'non_existent_table\' doesn\'t exist. Retrying... [9x]'
            ));
        }
    }
}
