<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Psr\Log\LoggerInterface;
use Retry\BackOff\BackOffPolicyInterface;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\RetryPolicyInterface;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;

class DbRetryProxy extends RetryProxy
{
    public const DEFAULT_MAX_TRIES = 5;

    /* set initial backoff to 1 second */
    public const DEFAULT_BACKOFF_INTERVAL = 1000;

    public const DEFAULT_EXPECTED_EXCEPTIONS = ['PDOException', 'ErrorException'];

    public function __construct(
        LoggerInterface $logger,
        ?int $maxTries = null,
        ?array $expectedExceptions = null,
        ?int $backoffInterval = null,
        ?RetryPolicyInterface $retryPolicy = null,
        ?BackoffPolicyInterface $backoffPolicy = null,
    ) {
        if ($retryPolicy === null) {
            $retryPolicy = new SimpleRetryPolicy(
                $maxTries ?? self::DEFAULT_MAX_TRIES,
                $expectedExceptions ?? self::DEFAULT_EXPECTED_EXCEPTIONS,
            );
        }
        if ($backoffPolicy === null) {
            $backoffPolicy = new ExponentialBackOffPolicy(
                $backoffInterval ?? self::DEFAULT_BACKOFF_INTERVAL,
            );
        }

        parent::__construct($retryPolicy, $backoffPolicy, $logger);
    }
}
