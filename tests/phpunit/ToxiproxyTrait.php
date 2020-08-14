<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Ihsw\Toxiproxy\Proxy;
use Ihsw\Toxiproxy\StreamDirections;
use Ihsw\Toxiproxy\Toxic;
use Ihsw\Toxiproxy\ToxicTypes;
use Ihsw\Toxiproxy\Toxiproxy;

trait ToxiproxyTrait
{
    protected Toxiproxy $toxiproxy;

    protected function getToxiproxyHost(): string
    {
        return 'toxiproxy';
    }

    protected function initToxiproxy(): void
    {
        $this->toxiproxy = new Toxiproxy('http://' . $this->getToxiproxyHost() . ':8474');
    }

    protected function createToxiproxyToDb(): Proxy
    {
        return $this->toxiproxy->create('mysql_proxy', 'mysql:3306');
    }

    protected function clearAllToxiproxies(): void
    {
        foreach ($this->toxiproxy->getAll() as $proxy) {
            $this->toxiproxy->delete($proxy);
        }
    }

    protected function simulateNetworkDown(Proxy $proxy): Toxic
    {
        // https://github.com/Shopify/toxiproxy#timeout
        return $proxy->create(ToxicTypes::TIMEOUT, StreamDirections::DOWNSTREAM, 1.0, [
            'timeout' => 10,
        ]);
    }

    protected function simulateNetworkLimitDataThenDown(Proxy $proxy, int $bytes): Toxic
    {
        // https://github.com/Shopify/toxiproxy#limit_data
        return $proxy->create('limit_data', StreamDirections::DOWNSTREAM, 1.0, [
            'bytes' => $bytes,
        ]);
    }
}
