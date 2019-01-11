<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Retry\RetryableException;
use Retry\Policy\CallableRetryPolicy;

class DbRetryPolicy extends CallableRetryPolicy
{

    public const DEFAULT_RETRYABLE_EXCEPTIONS = [
        \PDOException::class => ['^42.*'], // ignore all 42xxx level error codes
        \ErrorException::class => [] // don't ignore any ErrorExceptions
    ];

    public const DEFAULT_MAX_ATTEMPTS = 5;

    /** @var  array RetryableException[] */
    private $retryableExcpetions;

    public function __construct(?int $maxAttempts = null, ?array $retryableExceptions = null)
    {
        if (isset($retryableExceptions)) {
            $this->retryableExcpetions = $retryableExceptions;
        } else {
            $this->retryableExcpetions = array_map(
                function ($key, $value) {
                    return new RetryableException($key, $value);
                },
                array_keys(self::DEFAULT_RETRYABLE_EXCEPTIONS),
                self::DEFAULT_RETRYABLE_EXCEPTIONS
            );
        }
        parent::__construct(
            self::getDecider($this->retryableExcpetions),
            $maxAttempts ?? self::DEFAULT_MAX_ATTEMPTS
        );
    }

    public function addRetryableException(RetryableException $e): void
    {
        $this->retryableExcpetions[] = $e;
        $this->setShouldRetryMethod(self::getDecider($this->retryableExcpetions));
    }

    /**
     * @param array RetryableException[] $retryableExceptions
     * @return callable
     */
    public static function getDecider(array $retryableExceptions): callable
    {
        return function (\Throwable $e) use ($retryableExceptions): bool {
            foreach ($retryableExceptions as $retryableException) {
                $className = $retryableException->getClassName();
                if ($e instanceof $className) {
                    if ($retryableException->shouldThisExceptionBeRetried((string) $e->getCode())) {
                        return true;
                    }
                }
            }
            return false;
        };
    }
}
