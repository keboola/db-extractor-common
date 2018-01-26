<?php

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/12/15
 * Time: 14:26
 */

namespace Keboola\DbExtractor\Test;

use Symfony\Component\Yaml\Yaml;

class ExtractorTest extends \PHPUnit_Framework_TestCase
{
    protected $dataDir;

    protected $config;

    protected $driver = '';

    protected function setUp()
    {
        $this->dataDir = getenv('ROOT_PATH') . "/tests/data";
        parent::setUp();
    }

    protected function setupConfig($driver)
    {
        $this->config = Yaml::parse(file_get_contents($this->dataDir . '/' .$driver . '/config.yml'));
        $this->config['parameters']['data_dir'] = $this->dataDir;

        $this->config['parameters']['db']['user'] = $this->getEnv($driver, 'DB_USER', true);
        $this->config['parameters']['db']['#password'] = $this->getEnv($driver, 'DB_PASSWORD', true);
        $this->config['parameters']['db']['host'] = $this->getEnv($driver, 'DB_HOST');
        $this->config['parameters']['db']['port'] = $this->getEnv($driver, 'DB_PORT');
        $this->config['parameters']['db']['database'] = $this->getEnv($driver, 'DB_DATABASE');
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
