<?php

declare(strict_types=1);

require_once __DIR__ . '/../../vendor/autoload.php';

use Keboola\Component\UserException;
use Keboola\Component\Logger;
use Keboola\DbExtractor\Tests\CommonExtractor;

$logger = new Logger();

try {
    $commonExtractor = new CommonExtractor($logger);
    $commonExtractor->run();

    exit(0);
} catch (UserException $e) {
    $logger->error($e->getMessage());
    exit(1);
} catch (\Throwable $e) {
    $logger->critical(
        get_class($e) . ':' . $e->getMessage(),
        [
            'errFile' => $e->getFile(),
            'errLine' => $e->getLine(),
            'errCode' => $e->getCode(),
            'errTrace' => $e->getTraceAsString(),
            'errPrevious' => $e->getPrevious() ? get_class($e->getPrevious()) : '',
        ]
    );
    exit(2);
}
