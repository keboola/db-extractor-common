<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Tests;

use Keboola\Component\Logger;
use Keboola\DbExtractorCommon\BaseExtractor;
use PHPUnit\Framework\TestCase;

class ExtractorTest extends TestCase
{
    /** @var string */
    protected $dataDir = __DIR__ . "/Old/data";

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
            (string) file_get_contents($this->dataDir . '/' .$driver . '/exampleConfig.json'),
            true
        );
        $config['parameters']['db'] = $this->getConfigDbNode($driver);
        
        return $config;
    }

    protected function getConfigRow(string $driver): array
    {
        $config = json_decode(
            (string) file_get_contents($this->dataDir . '/' .$driver . '/exampleConfigRow.json'),
            true
        );

        $config['parameters']['db'] = $this->getConfigDbNode($driver);

        return $config;
    }

    protected function getConfigRowForCsvErr(string $driver): array
    {
        $config = json_decode(
            (string) file_get_contents($this->dataDir . '/' .$driver . '/exampleConfigRowCsvErr.json'),
            true
        );

        $config['parameters']['db'] = $this->getConfigDbNode($driver);

        return $config;
    }

    protected function getEnv(string $driver, string $suffix, bool $required = false): string
    {
        $env = strtoupper($driver) . '_' . $suffix;
        if ($required) {
            if (false === getenv($env)) {
                throw new \Exception($env . " environment variable must be set.");
            }
        }
        return (string) getenv($env);
    }

    public function getPrivateKey(string $driver): string
    {
        // docker-compose .env file does not support new lines in variables
        // so we have to modify the key https://github.com/moby/moby/issues/12997
        return str_replace('"', '', str_replace('\n', "\n", $this->getEnv($driver, 'DB_SSH_KEY_PRIVATE')));
    }

    protected function getCommonExtractor(array $config, array $state = []): CommonExtractor
    {
        putenv(sprintf('KBC_DATADIR=%s', $this->dataDir));
        $this->prepareConfigInDataDir($config);
        if (!empty($state)) {
            $this->prepareInputStateInDataDir($state);
        }

        $app = new CommonExtractor(new Logger());
        return $app;
    }

    protected function prepareConfigInDataDir(array $config): void
    {
        $configFilePath = $this->dataDir . DIRECTORY_SEPARATOR . 'config.json';
        file_put_contents(
            $configFilePath,
            json_encode($config, JSON_PRETTY_PRINT)
        );
    }

    protected function prepareInputStateInDataDir(array $state): void
    {
        $inputStateFilePath = $this->dataDir . DIRECTORY_SEPARATOR . 'in/state.json';
        file_put_contents(
            $inputStateFilePath,
            json_encode($state, JSON_PRETTY_PRINT)
        );
    }

    protected function runApplication(BaseExtractor $application): string
    {
        ob_start();
        $application->run();
        $result = ob_get_contents();
        ob_end_clean();
        return (string) $result;
    }
}
