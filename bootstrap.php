<?php

declare(strict_types=1);

if (!defined('ROOT_PATH')) {
    define('ROOT_PATH', __DIR__);
}

date_default_timezone_set('Europe/Prague');

error_reporting(E_ALL);
set_error_handler(function ($errno, $errstr, $errfile, $errline, array $errcontext): bool {
    if (!(error_reporting() & $errno)) {
        // respect error_reporting() level
        // libraries used in custom components may emit notices that cannot be fixed
        return false;
    }
    throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
});

require_once ROOT_PATH . '/vendor/autoload.php';
