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
        $logger = new TestLogger();
        $config = $this->createConfig();
        $config['image_parameters']['checkUsername'] = [
            'enabled' => true,
        ];
        putenv('KBC_REALUSER=' . (string) getEnv('COMMON_DB_USER'));
        new Application($config, $logger);

        Assert::assertTrue(
            $logger->hasInfoThatContains('Your username "root" and database username are same. Running allowed.')
        );
    }

    public function testInvalidCheckUsername(): void
    {
        $logger = new TestLogger();
        $config = $this->createConfig();
        $config['image_parameters']['checkUsername'] = [
            'enabled' => true,
        ];
        putenv('KBC_REALUSER=dbUsername');

        $this->expectException(BadUsernameException::class);
        $this->expectExceptionMessage(
            'Your username "dbUsername" does not have permission ' .
            'to run configuration with the database username "root"'
        );
        new Application($config, $logger);
    }

    public function testServiceAccountRegexpMatch(): void
    {
        $logger = new TestLogger();
        $config = $this->createConfig();
        $config['parameters']['db']['user'] = 'service__abc';
        $config['image_parameters']['checkUsername'] = [
            'enabled' => true,
            'serviceAccountRegexp' => '~^service__~i',
        ];
        putenv('KBC_REALUSER=dbUsername');

        new Application($config, $logger);

        Assert::assertTrue(
            $logger->hasInfoThatContains('Database username "service__abc" is service account, username check skipped.')
        );
    }

    public function testServiceAccountRegexpDontMatch(): void
    {
        $logger = new TestLogger();
        $config = $this->createConfig();
        $config['parameters']['db']['user'] = 'user123';
        $config['image_parameters']['checkUsername'] = [
            'enabled' => true,
            'serviceAccountRegexp' => '~^service__~i',
        ];
        putenv('KBC_REALUSER=dbUsername');

        $this->expectException(BadUsernameException::class);
        $this->expectExceptionMessage(
            'Your username "dbUsername" does not have permission ' .
            'to run configuration with the database username "user123"'
        );
        new Application($config, $logger);
    }

    public function testUserAccountRegexpMatch(): void
    {
        $logger = new TestLogger();
        $config = $this->createConfig();
        $config['parameters']['db']['user'] = 'user_abc';
        $config['image_parameters']['checkUsername'] = [
            'enabled' => true,
            'userAccountRegexp' => '~^user_~i',
        ];
        putenv('KBC_REALUSER=dbUsername');

        $this->expectException(BadUsernameException::class);
        $this->expectExceptionMessage(
            'Your username "dbUsername" does not have permission ' .
            'to run configuration with the database username "user_abc"'
        );
        new Application($config, $logger);
    }

    public function testUserAccountRegexpDontMatch(): void
    {
        $logger = new TestLogger();
        $config = $this->createConfig();
        $config['parameters']['db']['user'] = 'service__abc';
        $config['image_parameters']['checkUsername'] = [
            'enabled' => true,
            'userAccountRegexp' => '~^user_~i',
        ];
        putenv('KBC_REALUSER=dbUsername');

        new Application($config, $logger);

        Assert::assertTrue(
            $logger->hasInfoThatContains('Database username "service__abc" is service account, username check skipped.')
        );
    }

    public function testDisabledChecking(): void
    {
        $logger = new TestLogger();
        $config = $this->createConfig();
        putenv('KBC_REALUSER=' . (string) getEnv('COMMON_DB_USER'));
        new Application($config, $logger);

        Assert::assertEquals([], $logger->records);
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
            'image_parameters' => [],
        ];

        if ($action !== 'run') {
            $config['action'] = $action;
        }

        return $config;
    }
}
