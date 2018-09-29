<?php

declare(strict_types=1);

require_once __DIR__ . '/../../vendor/autoload.php';

use Keboola\DbExtractor\Application;
use Keboola\Component\UserException;
use Keboola\Component\Logger;
use Keboola\DbExtractor\Extractor\CommonExtractor;
use Keboola\DbExtractor\Extractor\ExtractorAdapter;

$datadirPath = rtrim((string) getenv('KBC_DATADIR'), '/');

$config = json_decode((string) file_get_contents($datadirPath . '/config.json'), true);
$logger = new Logger();


try {
    $commonExtractor = new CommonExtractor($logger, $config['parameters']['db']);

    // @todo - pass state from state.json
    $extractorAdapter = new ExtractorAdapter($commonExtractor, $logger, $config['action'], $config['parameters']);
    $extractorAdapter->run();

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


exit;

try {
    $app = new Application($config, $logger);
    $app->run();
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
