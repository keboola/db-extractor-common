<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Ihsw\Toxiproxy\Proxy;
use Keboola\Temp\Temp;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Process\Process;

class RetryTest extends TestCase
{
    use ToxiproxyTrait;
    use TestDataTrait;

    protected const LARGE_TABLE_NAME = 'large_test_table';
    protected const LARGE_TABLE_ROWS = 100000;

    protected Temp $temp;

    protected function setUp(): void
    {
        parent::setUp();
        $this->temp = new Temp();
        $this->initDatabase();
        $this->initToxiproxy();
    }

    protected function tearDown(): void
    {
        parent::tearDown();
        $this->temp->remove();
        $this->clearAllToxiproxies();
    }

    /**
     * - Network is DOWN
     * - Extractor is started
     * - Extractor failed, after 3x retries, when establishing a connection
     */
    public function testNetworkDown(): void
    {
        $proxy = $this->createToxiproxyToDb();

        // Network is down
        $this->simulateNetworkDown($proxy);

        // Extractor process is started
        $process = $this->createProcess($proxy);
        $process->start();

        // Extractor process ended, network is still down
        $process->wait();

        // Connect is retried 3x and then extraction failed
        // See DbConnection::CONNECT_MAX_RETRIES
        $output = $process->getOutput();
        $errorOutput = $process->getErrorOutput();
        Assert::assertFalse($process->isSuccessful());
        Assert::assertSame(1, $process->getExitCode());
        Assert::assertStringContainsString('MySQL server has gone away. Retrying... [1x]', $output);
        Assert::assertStringContainsString('MySQL server has gone away. Retrying... [2x]', $output);
        Assert::assertStringContainsString('Error connecting to DB:', $errorOutput);
        Assert::assertStringContainsString('MySQL server has gone away', $errorOutput);
    }

    /**
     * - Network is DOWN
     * - Extractor is started
     * - After 2 seconds is network connection restored, network is UP
     * - Extractor is successful, the logs shows that the retry was used
     */
    public function testNetworkDownUpDuringRetry(): void
    {
        $this->createLargeTable(self::LARGE_TABLE_ROWS, self::LARGE_TABLE_NAME);
        $proxy = $this->createToxiproxyToDb();

        // Network is down
        $toxic = $this->simulateNetworkDown($proxy);

        // Extractor process is started
        $process = $this->createProcess($proxy);
        $process->start();

        // Network is up - 2 seconds after the process started
        sleep(2);
        $proxy->delete($toxic);

        // Extractor process ended, network is up
        // It should be successful, because retry mechanism
        $process->wait();

        // Process is successful
        $output = $process->getOutput();
        $errorOutput = $process->getErrorOutput();
        Assert::assertTrue($process->isSuccessful());
        Assert::assertSame(0, $process->getExitCode());

        // Connect is successfully retried
        Assert::assertStringContainsString('MySQL server has gone away. Retrying... [1x]', $output);
        Assert::assertStringNotContainsString('Error connecting to DB:', $errorOutput); // not

        // Extraction is successful
        $this->assertOutputCsvValid();
    }

    /**
     * - Network is UP
     * - Extractor is started
     * - After 1MB of transmitted data is network DOWN
     * - Extractor failed, after 5x retries
     */
    public function testNetworkDownAfterLimit(): void
    {
        $this->createLargeTable(self::LARGE_TABLE_ROWS, self::LARGE_TABLE_NAME);
        $proxy = $this->createToxiproxyToDb();

        // Network will be down when transmitted data exceeded limit
        $bytes = 1 * 1024 * 1024; // 1MB
        $this->simulateNetworkLimitDataThenDown($proxy, $bytes);

        // Extractor process is started
        $process = $this->createProcess($proxy);
        $process->start();

        // Extractor process ended, network is still down
        $process->wait();

        // Query is retried 5x and then extraction failed
        // See DbConnection::DEFAULT_MAX_RETRIES
        $output = $process->getOutput();
        $errorOutput = $process->getErrorOutput();
        Assert::assertFalse($process->isSuccessful());
        Assert::assertSame(1, $process->getExitCode());
        Assert::assertStringContainsString('MySQL server has gone away', $output);
        Assert::assertStringContainsString('Retrying... [1x]', $output);
        Assert::assertStringContainsString('Retrying... [2x]', $output);
        Assert::assertStringContainsString('Retrying... [3x]', $output);
        Assert::assertStringContainsString('Retrying... [4x]', $output);
        Assert::assertStringContainsString('DB query failed:', $errorOutput);
        Assert::assertStringContainsString('MySQL server has gone away Tried 5 times.', $errorOutput);
    }

    /**
     * - Network is UP
     * - Extractor is started
     * - After 1MB of transmitted data is network DOWN
     * - After second retry is network connection restored, network is UP
     * - Extractor is successful, the logs shows that the retry was used
     */
    public function testNetworkDownAfterLimitUpDuringRetry(): void
    {
        $this->createLargeTable(self::LARGE_TABLE_ROWS, self::LARGE_TABLE_NAME);
        $proxy = $this->createToxiproxyToDb();

        // Network will be down when transmitted data exceeded limit
        $bytes = 1 * 1024 * 1024; // 1MB
        $toxic = $this->simulateNetworkLimitDataThenDown($proxy, $bytes);

        // Extractor process is started
        $process = $this->createProcess($proxy);
        $process->start(function (string $dest, string $msg) use ($proxy, $toxic): void {
            // After second retry is network connection restored
            if ($dest === 'out' && strpos($msg, 'Retrying... [2x]') !== false) {
                $proxy->delete($toxic);
            }
        });

        // Extractor process ended, network is up
        // It should be successful, because retry mechanism
        $process->wait();

        // Process is successful
        $output = $process->getOutput();
        $errorOutput = $process->getErrorOutput();
        Assert::assertTrue($process->isSuccessful());
        Assert::assertSame(0, $process->getExitCode());

        // Query is successfully retried
        $output = $process->getOutput();
        $errorOutput = $process->getErrorOutput();
        Assert::assertTrue($process->isSuccessful());
        Assert::assertSame(0, $process->getExitCode());
        Assert::assertStringContainsString('MySQL server has gone away', $output);
        Assert::assertStringContainsString('Retrying... [1x]', $output);
        Assert::assertStringContainsString('Retrying... [2x]', $output);
        Assert::assertStringNotContainsString('DB query failed:', $errorOutput); // not
        Assert::assertStringContainsString('Exported "100000" rows to "output".', $output);
        Assert::assertSame('', $errorOutput);

        // Extraction is successful
        $this->assertOutputCsvValid();
    }

    public function testRetriesDisabledForSyncActions(): void
    {
        $proxy = $this->createToxiproxyToDb();

        // Network is down
        $this->simulateNetworkLimitDataThenDown($proxy, 10);

        // Extractor process is started
        $process = $this->createProcess($proxy, 'testConnection');
        $process->start();

        // Extractor process ended, network is still down
        $process->wait();

        // No retries
        $output = $process->getOutput();
        $errorOutput = $process->getErrorOutput();
        Assert::assertFalse($process->isSuccessful());
        Assert::assertSame(1, $process->getExitCode());
        Assert::assertStringNotContainsString('Retrying', $output); // NOT contains
        Assert::assertStringContainsString('MySQL server has gone away', $errorOutput);
    }

    private function createProcess(Proxy $proxy, string $action = 'run'): Process
    {
        // Create config file
        file_put_contents(
            $this->temp->getTmpFolder() . '/config.json',
            json_encode($this->createConfig($proxy, $action))
        );

        // We run the extractor in a asynchronous process
        // so we can change the network parameters via Toxiproxy.
        return Process::fromShellCommandline(
            'php ' . __DIR__ . '/Fixtures/run.php',
            null,
            ['KBC_DATADIR' => $this->temp->getTmpFolder()]
        );
    }

    private function createConfig(Proxy $proxy, string $action = 'run'): array
    {
        $config = [
            'parameters' => [
                'db' => [
                    'host' => $this->getToxiproxyHost(),
                    'port' => $proxy->getListenPort(),
                    'database' => (string) getEnv('COMMON_DB_DATABASE'),
                    'user' => (string) getEnv('COMMON_DB_USER'),
                    '#password' => (string) getEnv('COMMON_DB_PASSWORD'),
                ],
                'id' => 123,
                'name' => 'row_name',
                'table' => [
                    'tableName' => self::LARGE_TABLE_NAME,
                    'schema' => (string) getEnv('COMMON_DB_DATABASE'),
                ],
                'outputTable' => 'output',
                'data_dir' => $this->temp->getTmpFolder(),
                'extractor_class' => 'Common',
            ],
        ];

        if ($action !== 'run') {
            $config['action'] = $action;
        }

        return $config;
    }

    private function assertOutputCsvValid(): void
    {
        $csvFile = $this->temp->getTmpFolder() . '/out/tables/output.csv';
        Assert::assertFileExists($csvFile);
        Assert::assertSame(
            self::LARGE_TABLE_ROWS . ' ' .$csvFile . "\n",
            Process::fromShellCommandline('wc -l ' . escapeshellarg($csvFile))->mustRun()->getOutput()
        );
    }
}
