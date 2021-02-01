<?php
declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\Temp\Temp;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Process\Process;

class CheckUsernameTest extends TestCase
{
    use TestDataTrait;

    private string $dbUsername;

    protected function setUp(): void
    {
        parent::setUp();
        $this->temp = new Temp();
        $this->initDatabase();
        $this->dbUsername = (string) getEnv('COMMON_DB_USER');
    }

    public function testValidCheckUsername(): void
    {
        $process = $this->createProcess(
            'run',
            ['KBC_REALUSER' => (string) getEnv('COMMON_DB_USER')]
        );
        $process->run();

        Assert::assertEquals(0, $process->getExitCode());
    }

    public function testInvalidCheckUsername(): void
    {
        $process = $this->createProcess(
            'run',
            ['KBC_REALUSER' => 'invalidUsername']
        );
        $process->run();

        Assert::assertEquals(1, $process->getExitCode());
        Assert::assertStringContainsString(
            'You do not have permission to run configuration with the database username "root"',
            $process->getErrorOutput()
        );
    }

    public function testTechnicalUsername(): void
    {
        $this->dbUsername = '_technicalUsername';

        $process = $this->createProcess(
            'run',
            ['KBC_REALUSER' => 'invalidUsername']
        );
        $process->run();

        Assert::assertEquals(1, $process->getExitCode());
        Assert::assertStringNotContainsString(
            'You do not have permission to run configuration with the database username "root"',
            $process->getErrorOutput()
        );

    }

    private function createProcess(string $action = 'run', array $additionalEnv = []): Process
    {
        // Create config file
        file_put_contents(
            $this->temp->getTmpFolder() . '/config.json',
            json_encode($this->createConfig($action))
        );

        // We run the extractor in a asynchronous process
        // so we can change the network parameters via Toxiproxy.
        return Process::fromShellCommandline(
            'php ' . __DIR__ . '/Fixtures/run.php',
            null,
            array_merge(
                ['KBC_DATADIR' => $this->temp->getTmpFolder()],
                $additionalEnv
            )
        );
    }

    private function createConfig(string $action = 'run'): array
    {
        $config = [
            'parameters' => [
                'db' => [
                    'host' => (string) getEnv('COMMON_DB_HOST'),
                    'port' => (string) getEnv('COMMON_DB_PORT'),
                    'database' => (string) getEnv('COMMON_DB_DATABASE'),
                    'user' => $this->dbUsername,
                    '#password' => (string) getEnv('COMMON_DB_PASSWORD'),
                ],
                'id' => 123,
                'name' => 'row_name',
                'table' => [
                    'tableName' => 'simple',
                    'schema' => (string) getEnv('COMMON_DB_DATABASE'),
                ],
                'outputTable' => 'output',
            ],
        ];

        if ($action !== 'run') {
            $config['action'] = $action;
        }

        return $config;
    }



}
