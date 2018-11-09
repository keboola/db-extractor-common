<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Retry;

use Retry\Policy\AbstractRetryPolicy;
use Retry\RetryContextInterface;

class ErrorCodeRetryPolicy extends AbstractRetryPolicy
{
    /**
     * The default limit to the number of attempts for a new policy.
     *
     * @var int
     */
    public const DEFAULT_MAX_ATTEMPTS = 3;

    /**
     * The maximum number of retry attempts before failure.
     *
     * @var int
     */
    private $maxAttempts;

    /**
     * The list of exceptions that can have ignorable error codes included
     *
     * @var array of RetryableException
     */
    private $retryableExceptions;

    /**
     * @param int        $maxAttempts The number of attempts before a retry becomes impossible.
     * @param array|null $retryableExceptions
     */
    public function __construct(
        ?int $maxAttempts = null,
        ?array $retryableExceptions = null
    ) {

        $this->maxAttempts = $maxAttempts ?? self::DEFAULT_MAX_ATTEMPTS;

        if ($retryableExceptions) {
            $this->retryableExceptions = $retryableExceptions;
        }
    }

    public function canRetry(RetryContextInterface $context): bool
    {
        $e = $context->getLastException();

        return (!$e || $this->shouldRetryForException($e)) && $context->getRetryCount() < $this->maxAttempts;
    }

    private function shouldRetryForException(\Exception $e): bool
    {
        foreach ($this->retryableExceptions as $retryableException) {
            $className = $retryableException->getClassName();
            if ($e instanceof $className) {
                if ($retryableException->shouldThisExceptionBeRetried((string) $e->getCode())) {
                    return true;
                }
            }
        }
        return false;
    }
}
