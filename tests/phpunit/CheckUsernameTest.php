<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Exception\BadUsernameException;
use Keboola\Temp\Temp;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

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
        putenv('KBC_REALUSER=' . (string) getEnv('COMMON_DB_USER'));
        new Application($this->createConfig(), new TestLogger());
        $this->expectNotToPerformAssertions();
    }

    public function testInvalidCheckUsername(): void
    {
        putenv('KBC_REALUSER=invalidUsername');

        $this->expectException(BadUsernameException::class);
        $this->expectExceptionMessage(
            'Your username "invalidUsername" does not have permission ' .
            'to run configuration with the database username "root"'
        );
        new Application($this->createConfig(), new TestLogger());
    }

    public function testTechnicalUsername(): void
    {
        putenv('KBC_REALUSER=invalidUsername');
        $config = $this->createConfig();
        $config['parameters']['db']['user'] = '_technicalUsername';

        $logger = new TestLogger();
        new Application($config, $logger);

        Assert::assertTrue(
            $logger->hasInfoThatContains('Starting export data with a service account "_technicalUsername".')
        );
    }

    public function testDisableChecking(): void
    {
        putenv('KBC_REALUSER=invalidUsername');
        $config = $this->createConfig();
        $config['image_parameters']['check_kbc_realuser'] = false;
        new Application($config, new TestLogger());

        $this->expectNotToPerformAssertions();
    }

    private function createConfig(string $action = 'run'): array
    {
        $config = [
            'parameters' => [
                'data_dir' => 'testDatadir',
                'extractor_class' => 'test',
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
            'image_parameters' => [
                'check_kbc_realuser' => true,
            ],
        ];

        if ($action !== 'run') {
            $config['action'] = $action;
        }

        return $config;
    }
}
