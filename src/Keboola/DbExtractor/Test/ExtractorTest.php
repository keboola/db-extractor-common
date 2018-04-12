<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Test;

use Symfony\Component\Yaml\Yaml;

class ExtractorTest extends \PHPUnit_Framework_TestCase
{
    protected $dataDir = ROOT_PATH . "/tests/data";

    /**
     * @param $driver
     * @param string $format (yaml or json)
     * @return mixed
     */
    protected function getConfig($driver, $format = 'yaml')
    {
        if ($format === 'json') {
            $config = json_decode(file_get_contents($this->dataDir . '/' .$driver . '/config.json'), true);
        } else {
            $config = Yaml::parse(file_get_contents($this->dataDir . '/' .$driver . '/config.yml'));
        }
        $config['parameters']['data_dir'] = $this->dataDir;

        $config['parameters']['db']['user'] = $this->getEnv($driver, 'DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv($driver, 'DB_PASSWORD', true);
        $config['parameters']['db']['host'] = $this->getEnv($driver, 'DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv($driver, 'DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv($driver, 'DB_DATABASE');

        $config['parameters']['extractor_class'] = ucfirst($driver);
        
        return $config;
    }

    protected function getConfigRow($driver)
    {
        $config = json_decode(file_get_contents($this->dataDir . '/' .$driver . '/configRow.json'), true);

        $config['parameters']['data_dir'] = $this->dataDir;

        $config['parameters']['db']['user'] = $this->getEnv($driver, 'DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv($driver, 'DB_PASSWORD', true);
        $config['parameters']['db']['host'] = $this->getEnv($driver, 'DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv($driver, 'DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv($driver, 'DB_DATABASE');

        $config['parameters']['extractor_class'] = ucfirst($driver);

        return $config;
    }

    protected function getEnv($driver, $suffix, $required = false)
    {
        $env = strtoupper($driver) . '_' . $suffix;
        if ($required) {
            if (false === getenv($env)) {
                throw new \Exception($env . " environment variable must be set.");
            }
        }
        return getenv($env);
    }

    public function getPrivateKey($driver)
    {
        // docker-compose .env file does not support new lines in variables so we have to modify the key https://github.com/moby/moby/issues/12997
        return str_replace('"', '', str_replace('\n', "\n", $this->getEnv($driver, 'DB_SSH_KEY_PRIVATE')));
    }
}
