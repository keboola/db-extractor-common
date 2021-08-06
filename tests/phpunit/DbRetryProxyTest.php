<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\DbRetryProxy;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PDOException;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class DbRetryProxyTest extends TestCase
{
    private TestHandler $testHandler;

    private Logger $logger;

    public function setUp(): void
    {
        $this->testHandler = new TestHandler();
        $this->logger = new Logger('dbRetryProxyTest');
        $this->logger->pushHandler($this->testHandler);
    }

    public function testMaxRetries(): void
    {
        $retryProxy = new DbRetryProxy(
            $this->logger
        );

        $i = 0;

        $res = $retryProxy->call(function () use (&$i): int {
            $i++;
            if ($i < 5) {
                throw new PDOException('test throw ' . $i);
            } else {
                return $i;
            }
        });
        Assert::assertEquals(5, $res);
        foreach ($this->testHandler->getRecords() as $ind => $record) {
            $tryNumber = $ind + 1;
            Assert::assertEquals("test throw {$tryNumber}. Retrying... [{$tryNumber}x]", $record['message']);
        }
    }
}
