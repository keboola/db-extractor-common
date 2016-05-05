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

        $this['action'] = isset($config['action'])?$config['action']:'run';

        $this['parameters'] = $config['parameters'];

        $this['logger'] = function() use ($app) {
            return new Logger(APP_NAME);
        };

        $this['extractor_factory'] = function() use ($app) {
            return new ExtractorFactory($app['parameters']);
        };

        $this['extractor'] = function() use ($app) {
            return $app['extractor_factory']->create($app['logger']);
        };

        $this->configDefinition = new ConfigDefinition();
    }

    public function run()
    {
        $this['parameters'] = $this->validateParameters($this['parameters']);

        $actionMethod = $this['action'] . 'Action';
        if (!method_exists($this, $actionMethod)) {
            throw new UserException(sprintf("Action '%s' does not exist."));
        }

        return $this->$actionMethod();
    }

    public function setConfigDefinition(ConfigurationInterface $definition)
    {
        $this->configDefinition = $definition;
    }

    private function validateParameters($parameters)
    {
        try {
            $processor = new Processor();
            $processedParameters = $processor->processConfiguration(
                $this->configDefinition,
                [$parameters]
            );

            if (!empty($processedParameters['db']['#password'])) {
                $processedParameters['db']['password'] = $processedParameters['db']['#password'];
            }

            return $processedParameters;
        } catch (ConfigException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }

    private function runAction()
    {
        $imported = [];
        $tables = array_filter($this['parameters']['tables'], function ($table) {
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

    private function testConnectionAction()
    {
        try {
            $this['extractor']->testConnection();
        } catch (\Exception $e) {
            throw new UserException(sprintf("Connection failed: '%s'", $e->getMessage()), 0, $e);
        }

        return [
            'status' => 'ok',
        ];
    }
}
