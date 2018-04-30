<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Exception\UserException;

class Retrier
{
    /**
     * @var  int
     */
    private $maxTries;

    /**
     * @var array
     */
    private $expectedExceptions;

    /**
     * @var Logger
     */
    private $logger;

    public function __construct(int $maxTries, array $expectedExceptions, Logger $logger)
    {
        $this->maxTries = $maxTries;
        $this->expectedExceptions = $expectedExceptions;
        $this->logger = $logger;
    }

    /**
     * @param callable $method
     * @param array    $methodParams
     * @return mixed
     * @throws UserException
     */
    public function retry(
        callable $method,
        array $methodParams
    ) {
        $counter = 0;

        while (true) {
            try {
                return call_user_func($method, $methodParams);
            } catch (\Throwable $e) {
                foreach ($this->expectedExceptions as $expectedException) {
                    if ($e instanceof $expectedException) {
                        $counter++;
                        if ($counter > $this->maxTries) {
                            throw $e;
                        }
                        $this->logger->info(sprintf('%s. Retrying... [%dx]', $e->getMessage(), $counter));
                    } else {
                        throw $e;
                    }
                }
            }
        }
    }
}
