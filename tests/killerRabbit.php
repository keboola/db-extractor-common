<?php

declare(strict_types=1);

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

$retries = 0;
while (true) {
    $result = $client->describeDBInstances(['DBInstanceIdentifier' => getenv('TEST_RDS_ID')]);
    $state = $result->toArray()['DBInstances'][0]['DBInstanceStatus'];
    if ($state != 'available') {
        echo sprintf('Instance is in state "%s".' . PHP_EOL, $state);
        sleep(5);
        $retries++;
        if ($retries > 10) {
            exit('Instance seems dead.');
        }
    } else {
        echo sprintf('Instance is available "%s".' . PHP_EOL, $state);
        break;
    }
}

echo sprintf('Sleeping %s seconds.' . PHP_EOL, $argv[1]);
sleep((int) $argv[1]);
echo 'Rebooting instance' . PHP_EOL;

$client->rebootDBInstance([
    'DBInstanceIdentifier' => getenv('TEST_RDS_ID'),
    'ForceFailover' => false,
]);

echo 'Rabbit of Caerbannog' . PHP_EOL;
