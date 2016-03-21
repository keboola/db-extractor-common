<?php

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/12/15
 * Time: 12:17
 */

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\ConfigDefinition;
use Keboola\DbExtractor\Exception\UserException;
use Pimple\Container;
use Symfony\Component\Config\Definition\ConfigurationInterface;
use Symfony\Component\Config\Definition\Exception\Exception as ConfigException;
use Symfony\Component\Config\Definition\Processor;

class Application extends Container
{
    private $configDefinition;

    public function __construct($config)
    {
        parent::__construct();

        $app = $this;

        $this['config'] = $config;

        $this['logger'] = function() use ($app) {
            return new Logger(APP_NAME);
        };

        $this['extractor_factory'] = function() use ($app) {
            return new ExtractorFactory($app['config']);
        };

        $this['extractor'] = function() use ($app) {
            return $app['extractor_factory']->create($app['logger']);
        };

        $this->configDefinition = new ConfigDefinition();
    }

    public function run()
    {
        $this['config'] = $this->validateConfig($this['config']);

        $imported = [];
        $tables = array_filter($this['config']['parameters']['tables'], function ($table) {
            return ($table['enabled']);
        });

        foreach ($tables as $table) {
            $imported[] = $this['extractor']->export($table);
        }

        return [
            'status' => 'ok',
            'imported' => $imported
        ];
    }

    public function setConfigDefinition(ConfigurationInterface $definition)
    {
        $this->configDefinition = $definition;
    }

    private function validateConfig($config)
    {
        try {
            $processor = new Processor();
            return $processor->processConfiguration(
                $this->configDefinition,
                [$config]
            );
        } catch (ConfigException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }
}
