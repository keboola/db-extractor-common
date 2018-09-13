<?php

require __DIR__ . '/../vendor/autoload.php';

if (!isset($argv[1]) || (intval($argv[1]) != $argv[1])) {
    exit('Enter number of seconds to sleep as the first argument.');
}

$client = new \Aws\Rds\RdsClient([
    'credentials' => [
        'key' => getenv('TEST_RDS_ACCESS_KEY'),
        'secret' => getenv('TEST_RDS_ACCESS_SECRET'),
    ],
    'region' => 'us-east-1',
    'version' => '2014-10-31',
]);

echo 'Sleeping ' . $argv[1] . ' seconds.';
sleep($argv[1]);
echo 'Rebooting instance';

$client->rebootDBInstance([
    'DBInstanceIdentifier' => getenv('TEST_RDS_ID'),
    'ForceFailover' => false,
]);

echo 'Rabbit of Caerbannog';
