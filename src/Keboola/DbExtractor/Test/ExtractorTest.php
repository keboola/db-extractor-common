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
    protected $dataDir = ROOT_PATH . "/tests/data";

    protected function getConfig($driver)
    {
        $config = Yaml::parse(file_get_contents($this->dataDir . '/' .$driver . '/config.yml'));
        $config['parameters']['data_dir'] = $this->dataDir;

        $config['parameters']['db']['user'] = $this->getEnv($driver, 'DB_USER', true);
        $config['parameters']['db']['password'] = $this->getEnv($driver, 'DB_PASSWORD', true);
        $config['parameters']['db']['host'] = $this->getEnv($driver, 'DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv($driver, 'DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv($driver, 'DB_DATABASE');

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
}
