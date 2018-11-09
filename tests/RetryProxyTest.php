<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Logger;
use Keboola\DbExtractor\Retry\RetryableException;
use Keboola\DbExtractor\Retry\RetryProxy;
use Keboola\DbExtractor\Test\DataLoader;
use Keboola\DbExtractor\Test\ExtractorTest;
use Monolog\Handler\TestHandler;

class RetryProxyTest extends ExtractorTest
{
    public const DRIVER = 'common';
    /**
     * @var  \PDO
     */
    private $db;

    public function setUp(): void
    {
        $dataLoader = new DataLoader(
            $this->getEnv(self::DRIVER, 'DB_HOST'),
            $this->getEnv(self::DRIVER, 'DB_PORT'),
            $this->getEnv(self::DRIVER, 'DB_DATABASE'),
            $this->getEnv(self::DRIVER, 'DB_USER'),
            $this->getEnv(self::DRIVER, 'DB_PASSWORD')
        );
        $this->db = $dataLoader->getPdo();
        $this->db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
    }

    /**
     * @dataProvider ignorableExceptionsProvider
     */
    public function testSkipRetryFor42(RetryableException $retryableException): void
    {
        $logger = new Logger('test-retry-proxy-logger');
        $testHandler = new TestHandler();
        $logger->pushHandler($testHandler);

        $retryProxy = new RetryProxy(
            $logger,
            RetryProxy::DEFAULT_MAX_TRIES,
            RetryProxy::DEFAULT_BACKOFF_INTERVAL,
            [$retryableException]
        );

        try {
            $retryProxy->call(function (): void {
                $this->db->query('SELECT SOMETHING FROM NOTHING;');
            });
        } catch (\Throwable $e) {
            $this->assertContains("SQLSTATE[42S02]: Base table or view not found", $e->getMessage());
            $this->assertFalse($testHandler->hasInfoThatContains('Retrying'));
        }
    }

    /**
     * @dataProvider retryableErrorCodesProvider
     */
    public function testDoesRetry(RetryableException $retryableException): void
    {
        $logger = new Logger('test-retry-proxy-logger');
        $testHandler = new TestHandler();
        $logger->pushHandler($testHandler);

        $retryProxy = new RetryProxy(
            $logger,
            RetryProxy::DEFAULT_MAX_TRIES,
            RetryProxy::DEFAULT_BACKOFF_INTERVAL,
            [$retryableException]
        );

        try {
            $retryProxy->call(function (): void {
                $this->db->query('SELECT SOMETHING FROM NOTHING;');
            });
        } catch (\Throwable $e) {
            $this->assertTrue($testHandler->hasInfoThatContains('Retrying'));
        }
    }

    public function retryableErrorCodesProvider(): array
    {
        return [
            [
                new RetryableException(\PDOException::class, ['82000', '64000']),
            ],
        ];
    }

    public function ignorableErrorCodesProvider(): array
    {
        return [
            [new RetryableException(\PDOException::class, ['42S02'])],
            [new RetryableException(\PDOException::class, ['^42.*'])],
        ];
    }
}
