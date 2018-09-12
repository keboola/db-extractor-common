<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\Functional;

use Keboola\DatadirTests\AbstractDatadirTestCase;
use Keboola\DatadirTests\DatadirTestSpecification;
use Keboola\DbExtractor\Test\DataLoader;

class DatadirTest extends AbstractDatadirTestCase
{
    protected function getScript(): string
    {
        return $this->getTestFileDir() . '/run.php';
    }

    private function getConfig(string $testDirectory): array
    {
        $configuration = json_decode((string) file_get_contents($testDirectory . '/config.json'), true);
        $configuration['parameters']['data_dir'] = $testDirectory;
        $configuration['parameters']['extractor_class'] = 'Common';
        return$configuration;
    }

    public function testActionTestConnection(): void
    {
        $testDirectory = __DIR__ . '/action-test-connection';

        $configuration = $this->getConfig($testDirectory);

        $credentials = $configuration['parameters']['db'];
        $dataLoader = new DataLoader(
            $credentials['host'],
            $credentials['port'],
            $credentials['database'],
            $credentials['user'],
            $credentials['#password']
        );
        $dataLoader->getPdo()->exec(sprintf(
            "DROP DATABASE IF EXISTS `%s`",
            $credentials['database']
        ));
        $dataLoader->getPdo()->exec(sprintf(
            "CREATE DATABASE `%s`
            DEFAULT CHARACTER SET utf8
            DEFAULT COLLATE utf8_general_ci",
            $credentials['database']
        ));

        $response = ['status' => 'success'];

        $specification = new DatadirTestSpecification(
            $testDirectory . '/source/data',
            0,
            json_encode($response, JSON_PRETTY_PRINT),
            null,
            $testDirectory . '/expected/data/out'
        );
        $tempDatadir = $this->getTempDatadir($specification);

        $configuration['parameters']['db'] = $credentials;
        file_put_contents(
            $tempDatadir->getTmpFolder() . '/config.json',
            json_encode($configuration, JSON_PRETTY_PRINT)
        );
        $process = $this->runScript($tempDatadir->getTmpFolder());
        $this->assertMatchesSpecification($specification, $process, $tempDatadir->getTmpFolder());
    }
}
