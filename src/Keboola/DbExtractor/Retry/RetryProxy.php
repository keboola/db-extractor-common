<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Retry;

use Keboola\DbExtractor\Logger;
use Retry\Policy\RetryPolicyInterface;
use Retry\Policy\SimpleRetryPolicy;
use Retry\BackOff\BackOffPolicyInterface;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\RetryProxyInterface;
use Retry\TerminatedRetryException;
use Retry\RetryException;

class RetryProxy implements RetryProxyInterface
{
    public const DEFAULT_MAX_TRIES = 5;

    public const DEFAULT_BACKOFF_INTERVAL = 100;

    public const DEFAULT_EXCEPTED_EXCEPTIONS = ['PDOException', 'ErrorException'];

    /** @var RetryPolicyInterface */
    private $retryPolicy;

    /** @var BackOffPolicyInterface */
    private $backOffPolicy;

    /** @var Logger */
    private $logger;

    public function __construct(
        Logger $logger,
        ?int $maxTries = null,
        ?int $backoffInterval = null,
        ?array $expectedExceptions = null,
        ?RetryPolicyInterface $retryPolicy = null,
        ?BackoffPolicyInterface $backoffPolicy = null
    ) {
        if ($retryPolicy === null) {
            $retryPolicy = new SimpleRetryPolicy(
                $maxTries ?? self::DEFAULT_MAX_TRIES,
                $expectedExceptions ? $expectedExceptions : self::DEFAULT_EXCEPTED_EXCEPTIONS
            );
        }
        if ($backoffPolicy === null) {
            $backoffPolicy = new ExponentialBackOffPolicy(
                $backoffInterval ? $backoffInterval : self::DEFAULT_BACKOFF_INTERVAL
            );
        }
        $this->retryPolicy   = $retryPolicy;
        $this->backOffPolicy = $backoffPolicy;
        $this->logger = $logger;
    }

    /**
     * Executing the action until it either succeeds or the policy dictates that we stop,
     * in which case the most recent exception thrown by the action will be rethrown.
     *
     * @param callable $action
     * @param array $arguments
     * @return mixed
     * @throws \Exception
     */
    public function call(callable $action, array $arguments = [])
    {
        $retryContext   = $this->retryPolicy->open();
        $backOffContext = $this->backOffPolicy->start($retryContext);
        do {
            try {
                return call_user_func_array($action, $arguments);
            } catch (\Exception $thrownException) {
                try {
                    $this->retryPolicy->registerException($retryContext, $thrownException);
                } catch (\Throwable $policyException) {
                    throw new TerminatedRetryException('Terminated retry after error in policy.');
                }
            }
            if ($this->retryPolicy->canRetry($retryContext)) {
                $this->logger->info(
                    sprintf(
                        '%s. Retrying... [%dx]',
                        $thrownException->getMessage(),
                        $retryContext->getRetryCount()
                    )
                );
                $this->backOffPolicy->backOff($backOffContext);
            }
        } while ($this->retryPolicy->canRetry($retryContext));

        if ($lastException = $retryContext->getLastException()) {
            throw $lastException;
        }
        throw new RetryException('Action call is failed.');
    }
}
