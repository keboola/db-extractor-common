<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Logger\LineFormatter;
use Monolog\Handler\StreamHandler;
use Monolog\Logger as MonologLogger;

class Logger extends MonologLogger
{
    public function __construct(string $name = '', bool $debug = false)
    {
        parent::__construct($name);

        $criticalHandler = self::getDefaultCriticalHandler();
        $errorHandler = self::getDefaultErrorHandler();
        $logHandler = self::getDefaultLogHandler($debug);
        $handlers = [
            $criticalHandler,
            $errorHandler,
            $logHandler,
        ];

        $this->setHandlers($handlers);
    }


    public static function getDefaultErrorHandler(): StreamHandler
    {
        $errorHandler = new StreamHandler('php://stderr');
        $errorHandler->setBubble(false);
        $errorHandler->setLevel(MonologLogger::WARNING);
        $errorHandler->setFormatter(new LineFormatter("%message%\n"));
        return $errorHandler;
    }
    public static function getDefaultLogHandler(bool $debug = false): StreamHandler
    {
        $logHandler = new StreamHandler('php://stdout');
        $logHandler->setBubble(false);
        $logHandler->setLevel(($debug) ? MonologLogger::DEBUG : MonologLogger::INFO);
        $logHandler->setFormatter(new LineFormatter("%message%\n"));
        return $logHandler;
    }
    public static function getDefaultCriticalHandler(): StreamHandler
    {
        $handler = new StreamHandler('php://stderr');
        $handler->setBubble(false);
        $handler->setLevel(MonologLogger::CRITICAL);
        $handler->setFormatter(new LineFormatter("[%datetime%] %level_name%: %message% %context% %extra%\n"));
        return $handler;
    }
}
