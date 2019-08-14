<?php


namespace Keboola\DbExtractor\Tests;


use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Test\ExtractorTest;

class ConfigurationTest extends ExtractorTest
{
    public function testExtraKeysInRowActionConfig(): void
    {
        $config = $this->getConfigRow('common');
        $config['action'] = 'getTables';
        $config['parameters']['somethingExtra'] = 'some extra parameter';
        $app = parent::getApplication('db-ex-common', $config);
        $result = $app->run();
        $this->assertEquals('success', $result['status']);
    }
}