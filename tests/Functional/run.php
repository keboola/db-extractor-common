<?php

declare(strict_types=1);

require_once __DIR__ . '/../../vendor/autoload.php';

use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;

$datadirPath = rtrim((string) getenv('KBC_DATADIR'), '/');

$config = json_decode((string) file_get_contents($datadirPath . '/config.json'), true);
$logger = new Logger('datadir-tests');


try {
    $app = new Application($config, $logger);
    echo json_encode($app->run(), JSON_PRETTY_PRINT);
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
