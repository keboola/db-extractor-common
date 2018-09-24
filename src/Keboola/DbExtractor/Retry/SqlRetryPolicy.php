<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Retry;

use Retry\Policy\AbstractRetryPolicy;
use Retry\RetryContextInterface;

class SqlRetryPolicy extends AbstractRetryPolicy
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
     * The list of retryable exceptions
     *
     * @var array
     */
    private $retryableExceptions = ['Exception'];

    /**
     * The list of SQLSTATE code masks to NOT retry
     *
     * @var array
     */
    private $ignorableSqlstateMasks = [];

    /**
     * @param int        $maxAttempts The number of attempts before a retry becomes impossible.
     * @param array|null $retryableExceptions
     */
    public function __construct(
        ?int $maxAttempts = null,
        ?array $retryableExceptions = null,
        ?array $ignorableSqlstateMasks = null
    ) {
        if ($maxAttempts === null) {
            $maxAttempts = self::DEFAULT_MAX_ATTEMPTS;
        }

        $this->maxAttempts = (int) $maxAttempts;

        if ($retryableExceptions) {
            $this->retryableExceptions = $retryableExceptions;
        }

        if ($ignorableSqlstateMasks) {
            $this->ignorableSqlstateMasks = $ignorableSqlstateMasks;
        }
    }

    public function canRetry(RetryContextInterface $context): bool
    {
        $e = $context->getLastException();

        return (!$e || $this->shouldRetryForException($e)) && $context->getRetryCount() < $this->maxAttempts;
    }

    private function shouldRetryForException(\Exception $e): bool
    {
        foreach ($this->retryableExceptions as $class) {
            if (is_a($e, $class)) {
                if ($this->shouldIgnoreForSqlStateCode($e->getMessage())) {
                    return false;
                }
                return true;
            }
        }
        return false;
    }

    private function shouldIgnoreForSqlStateCode(string $errorMessage): bool
    {
        if (strstr($errorMessage, 'SQLSTATE') === false) {
            return false;
        }
        preg_match('/SQLSTATE\[(\w+)\] (.*)/', $errorMessage(), $matches);
        if (count($matches) < 2) {
            return false;
        }
        if (in_array($matches[1], $this->ignorableSqlstateMasks)) {
            return true;
        }
        return false;
    }
}
