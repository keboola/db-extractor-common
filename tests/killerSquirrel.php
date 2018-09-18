<?php

declare(strict_types=1);

if (!isset($argv[1]) || (intval($argv[1]) != $argv[1])) {
    exit('Enter number of seconds to sleep as the first argument.' . PHP_EOL);
}
if (!isset($argv[2]) || (intval($argv[2]) != $argv[2])) {
    exit('Enter the port as the second argument.' . PHP_EOL);
}

echo sprintf('Sleeping %s seconds.' . PHP_EOL, $argv[1]);
sleep((int) $argv[1]);
echo 'Killing connection' . PHP_EOL;

// the timeout is the time for which connections are killed
exec('timeout 10 tcpkill -9 port ' . $argv[2] . ' 2>&1', $output, $code);
echo 'Code: ' . $code . ' Output: ' . implode(', ', $output) .  PHP_EOL;

echo 'Squirrel of Caerbannog' . PHP_EOL;
