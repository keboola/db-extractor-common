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
    protected $dataDir = __DIR__ . "/../../../../tests/data";

    protected function getConfig($driver)
    {
        $config = Yaml::parse(file_get_contents($this->dataDir . '/' .$driver . '/config.yml'));
        $config['dataDir'] = $this->dataDir;

        if (false === getenv(strtoupper($driver) . '_DB_USER')) {
            throw new \Exception("DB_USER envrionment variable must be set.");
        }

        if (false === getenv(strtoupper($driver) . '_DB_PASSWORD')) {
            throw new \Exception("DB_PASSWORD envrionment variable must be set.");
        }

        $config['parameters']['db']['user'] = getenv(strtoupper($driver) . '_DB_USER');
        $config['parameters']['db']['password'] = getenv(strtoupper($driver) . '_DB_PASSWORD');

        if (false !== getenv(strtoupper($driver) . '_DB_HOST')) {
            $config['parameters']['db']['host'] = getenv(strtoupper($driver) . '_DB_HOST');
        }

        if (false !== getenv(strtoupper($driver) . '_DB_PORT')) {
            $config['parameters']['db']['port'] = getenv(strtoupper($driver) . '_DB_PORT');
        }

        if (false !== getenv(strtoupper($driver) . '_DB_DATABASE')) {
            $config['parameters']['db']['database'] = getenv(strtoupper($driver) . '_DB_DATABASE');
        }

        return $config;
    }
}