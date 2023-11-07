<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Test;

use Exception;
use Keboola\Component\JsonHelper;
use Keboola\DbExtractor\Application;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class ExtractorTest extends TestCase
{
    protected string $dataDir = '/code/tests/phpunit/data';

    protected string $appName = 'ex-db-common';

    protected function setUp(): void
    {
        putenv('KBC_DATADIR=' . $this->dataDir);
    }

    protected function getConfigDbNode(string $driver): array
    {
        return [
            'user' => $this->getEnv($driver, 'DB_USER', true),
            '#password' => $this->getEnv($driver, 'DB_PASSWORD', true),
            'host' => $this->getEnv($driver, 'DB_HOST'),
            'port' => $this->getEnv($driver, 'DB_PORT'),
            'database' => $this->getEnv($driver, 'DB_DATABASE'),
        ];
    }

    protected function getConfig(string $driver): array
    {
        $config = json_decode(
            (string) file_get_contents($this->dataDir . '/' .$driver . '/config.json'),
            true,
        );
        $config['parameters']['data_dir'] = $this->dataDir;
        $config['parameters']['db'] = $this->getConfigDbNode($driver);
        $config['parameters']['extractor_class'] = ucfirst($driver);

        return $config;
    }

    protected function getConfigRow(string $driver): array
    {
        $config = json_decode(
            (string) file_get_contents($this->dataDir . '/' .$driver . '/configRow.json'),
            true,
        );

        $config['parameters']['data_dir'] = $this->dataDir;
        $config['parameters']['db'] = $this->getConfigDbNode($driver);
        $config['parameters']['extractor_class'] = ucfirst($driver);

        return $config;
    }

    protected function getConfigRowForCsvErr(string $driver): array
    {
        $config = json_decode(
            (string) file_get_contents($this->dataDir . '/' .$driver . '/configRowCsvErr.json'),
            true,
        );

        $config['parameters']['data_dir'] = $this->dataDir;
        $config['parameters']['db'] = $this->getConfigDbNode($driver);
        $config['parameters']['extractor_class'] = ucfirst($driver);

        return $config;
    }

    protected function getEnv(string $driver, string $suffix, bool $required = false): string
    {
        $env = strtoupper($driver) . '_' . $suffix;
        if ($required) {
            if (!getenv($env)) {
                throw new Exception($env . ' environment variable must be set.');
            }
        }
        return (string) getenv($env);
    }

    public function getPrivateKey(): string
    {
        return (string) file_get_contents('/root/.ssh/id_rsa');
    }

    public function getPublicKey(): string
    {
        return (string) file_get_contents('/root/.ssh/id_rsa.pub');
    }

    protected function getApp(array $config, ?LoggerInterface $logger = null, array $state = []): Application
    {
        JsonHelper::writeFile($this->dataDir . '/config.json', $config);
        if ($state) {
            JsonHelper::writeFile($this->dataDir . '/in/state.json', $state);
        }
        return self::getApplication($this->appName, $logger);
    }

    protected function getApplication(
        string $appName,
        ?LoggerInterface $logger = null,
    ): Application {
        $logger = $logger ?? new Logger($appName);
        return new Application($logger);
    }
}
