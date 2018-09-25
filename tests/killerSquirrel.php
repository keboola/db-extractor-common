<?php

declare(strict_types=1);

if (!isset($argv[1]) || (intval($argv[1]) != $argv[1])) {
    exit('Enter number of seconds to sleep before network kill as the first argument.' . PHP_EOL);
}
if (!isset($argv[2]) || (intval($argv[2]) != $argv[2])) {
    exit('Enter number of seconds to keep the connection down as the second argument.' . PHP_EOL);
}

sleep((int) $argv[1]);
echo 'Killing connection' . PHP_EOL;

// the timeout is the time for which connections are killed
exec('docker network disconnect db-extractor-common_db_network db_tests', $output, $code);
//exec('docker network connect host db_tests', $output, $code);
//exec('timeout 10 tcpkill -9 port ' . $argv[2] . ' 2>&1', $output, $code);
echo 'Code: ' . $code . ' Output: ' . implode(', ', $output) .  PHP_EOL;
echo 'Connection killed, sleeping before recreating' . PHP_EOL;

sleep((int) $argv[2]);
//exec('docker network disconnect none db_tests', $output, $code);
exec('docker network connect db-extractor-common_db_network db_tests', $output, $code);
echo 'Code: ' . $code . ' Output: ' . implode(', ', $output) .  PHP_EOL;

echo 'Squirrel of Caerbannog' . PHP_EOL;
