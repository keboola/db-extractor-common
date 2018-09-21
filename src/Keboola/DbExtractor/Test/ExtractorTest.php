<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Test;

use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Yaml\Yaml;

class ExtractorTest extends TestCase
{
    public const CONFIG_FORMAT_YAML = 'yaml';
    public const CONFIG_FORMAT_JSON = 'json';

    /** @var string */
    protected $dataDir = __DIR__ . "/../../../../tests/data";

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

    protected function getConfig(string $driver, string $format = self::CONFIG_FORMAT_YAML): array
    {
        switch ($format) {
            case self::CONFIG_FORMAT_JSON:
                $config = json_decode(
                    (string) file_get_contents($this->dataDir . '/' .$driver . '/config.json'),
                    true
                );
                break;
            case self::CONFIG_FORMAT_YAML:
                $config = Yaml::parse(
                    (string) file_get_contents($this->dataDir . '/' .$driver . '/config.yml')
                );
                break;
            default:
                throw new UserException("Unsupported configuration format: " . $format);
        }
        $config['parameters']['data_dir'] = $this->dataDir;
        $config['parameters']['db'] = $this->getConfigDbNode($driver);
        $config['parameters']['extractor_class'] = ucfirst($driver);
        
        return $config;
    }

    protected function getConfigRow(string $driver): array
    {
        $config = json_decode((string) file_get_contents($this->dataDir . '/' .$driver . '/configRow.json'), true);

        $config['parameters']['data_dir'] = $this->dataDir;
        $config['parameters']['db'] = $this->getConfigDbNode($driver);
        $config['parameters']['extractor_class'] = ucfirst($driver);

        return $config;
    }

    protected function getConfigRowForCsvErr(string $driver): array
    {
        $config = json_decode(file_get_contents($this->dataDir . '/' .$driver . '/configRowCsvErr.json'), true);

        $config['parameters']['data_dir'] = $this->dataDir;
        $config['parameters']['db'] = $this->getConfigDbNode($driver);
        $config['parameters']['extractor_class'] = ucfirst($driver);

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

    protected function getApplication(string $appName, array $config, array $state = []): Application
    {
        return new Application($config, new Logger($appName), $state);
    }
}
