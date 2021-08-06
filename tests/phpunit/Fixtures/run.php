<?php

declare(strict_types=1);

use Keboola\CommonExceptions\UserExceptionInterface;
use Keboola\Component\JsonHelper;
use Keboola\Component\Logger;
use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Exception\UserException;

require __DIR__ . '/../../../vendor/autoload.php';

// Standard run.php for Common (example) db extractor,
// used to test Common extractor in process

$logger = new Logger();

try {
    $dataFolder = getenv('KBC_DATADIR') === false ? '/data/' : (string) getenv('KBC_DATADIR');
    if (file_exists($dataFolder . '/config.json')) {
        $config = JsonHelper::readFile($dataFolder . '/config.json');
    } else {
        throw new UserException('Configuration file not found.');
    }

    // get the state
    $inputStateFile = $dataFolder . '/in/state.json';
    if (file_exists($inputStateFile)) {
        $inputState = JsonHelper::readFile($inputStateFile);
    } else {
        $inputState = [];
    }

    $config['parameters']['data_dir'] = $dataFolder;
    $config['parameters']['extractor_class'] = 'Common';
    $app = new Application($config, $logger, $inputState);
    $result = $app->run();

    if ($app['action'] !== 'run') {
        // Print sync action result
        echo JsonHelper::encode($result);
    } elseif (!empty($result['state'])) {
        // Write state if present
        $outputStateFile = $dataFolder . '/out/state.json';
        JsonHelper::writeFile($outputStateFile, $result['state']);
    }
    $logger->log('info', 'Extractor finished successfully.');
    exit(0);
} catch (UserExceptionInterface $e) {
    $logger->error($e->getMessage());
    exit(1);
} catch (Throwable $e) {
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
