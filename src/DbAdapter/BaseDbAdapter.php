<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\DbAdapter;

use Keboola\DbExtractor\Exception\DeadConnectionException;
use Retry\RetryProxy;
use Throwable;
use Psr\Log\LoggerInterface;
use Keboola\DbExtractor\Exception\DbAdapterException;
use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\DbAdapter\QueryResult\QueryResult;

abstract class BaseDbAdapter implements DbAdapter
{
    protected LoggerInterface $logger;

    abstract protected function createConnection(): void;

    abstract protected function doQuery(string $query): QueryResult;

    abstract protected function getExpectedExceptionClasses(): array;

    public function __construct(LoggerInterface $logger)
    {
        $this->logger = $logger;
        $this->createConnection();
    }

    public function query(string $query, int $maxRetries): QueryResult
    {
        return $this->callWithRetry(
            $maxRetries,
            function () use ($query) {
                return $this->doQueryReconnectOnError($query);
            }
        );
    }

    /**
     * @param callable $processor (QueryResult $dbResult): array
     * @return mixed - returned value from $processor
     */
    public function queryAndProcess(string $query, int $maxRetries, callable $processor)
    {
        return $this->callWithRetry(
            $maxRetries,
            function () use ($query, $processor) {
                $dbResult = $this->doQueryReconnectOnError($query);
                // A db error can occur during fetching, so it must be wrapped/retried together
                $result = $processor($dbResult);
                // Success of isAlive means that all data has been extracted
                $this->isAlive();
                return $result;
            }
        );
    }

    protected function doQueryReconnectOnError(string $query): QueryResult
    {
        try {
            return $this->doQuery($query);
        } catch (Throwable $e) {
            try {
                // Reconnect
                $this->createConnection();
            } catch (Throwable $e) {
            };
            throw $e;
        }
    }

    /**
     * @return mixed
     */
    protected function callWithRetry(int $maxRetries, callable $callback)
    {
        try {
            return $this
                ->createRetryProxy($maxRetries)
                ->call($callback);
        } catch (Throwable $e) {
            throw in_array(get_class($e), $this->getExpectedExceptionClasses(), true) ?
                new DbAdapterException($e->getMessage(), 0, $e) :
                $e;
        }
    }

    protected function createRetryProxy(int $maxRetries): RetryProxy
    {
        return new DbRetryProxy($this->logger, max(1, $maxRetries), $this->getExpectedExceptionClasses());
    }

    protected function isAlive(): void
    {
        try {
            $this->testConnection();
        } catch (DbAdapterException $e) {
            throw new DeadConnectionException('Dead connection: ' . $e->getMessage());
        }
    }
}
