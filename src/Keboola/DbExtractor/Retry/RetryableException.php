<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Retry;

class RetryableException
{
    /** @var string */
    private $className;

    /** @var array -- An array of regular expression matches for error codes to NOT retry */
    private $ignorableErrorCodes = [];

    public function __construct(string $className, array $ignorableErrorCodes = [])
    {
        $this->className = $className;
        $this->ignorableErrorCodes = $ignorableErrorCodes;
    }

    public function shouldThisExceptionBeRetried(string $errorCode): bool
    {
        foreach ($this->ignorableErrorCodes as $ignorableExpression) {
            preg_match('/' . $ignorableExpression . '/', $errorCode, $ignorableMatches);
            if (count($ignorableMatches) > 0) {
                return false;
            }
        }
        return true;
    }
}
