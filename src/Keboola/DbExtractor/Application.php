<?php

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/12/15
 * Time: 12:17
 */

namespace Keboola\DbExtractor;

use JsonSchema\Exception\InvalidConfigException;
use Keboola\DbExtractor\Configuration\ConfigDefinition;
use Keboola\DbExtractor\Configuration\ConfigRowDefinition;
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

        $this['logger'] = function () use ($app) {
            return new Logger(APP_NAME);
        };

        $this['extractor_factory'] = function () use ($app) {
            return new ExtractorFactory($app['parameters']);
        };

        $this['extractor'] = function () use ($app) {
            return $app['extractor_factory']->create($app['logger']);
        };
        if (isset($this['parameters']['tables'])) {
            $this->configDefinition = new ConfigDefinition();
        } else {
            $this->configDefinition = new ConfigRowDefinition();
        }
    }

    public function run()
    {
        $this['parameters'] = $this->validateParameters($this['parameters']);

        $actionMethod = $this['action'] . 'Action';
        if (!method_exists($this, $actionMethod)) {
            throw new UserException(sprintf("Action '%s' does not exist.", $this['action']));
        }

        return $this->$actionMethod();
    }

    public function setConfigDefinition(ConfigurationInterface $definition)
    {
        $this->configDefinition = $definition;
    }

    private function isTableValid($table)
    {
        if (!array_key_exists('schema', $table)) {
            return false;
        }
        if (!array_key_exists('tableName', $table)) {
            return false;
        }
        if ($table['tableName'] === '') {
            return false;
        }
        return true;
    }

    private function validateTableParameters(array $table)
    {
        if (isset($table['query']) && $table['query'] !== '') {
            if (isset($table['table'])) {
                throw new ConfigException(sprintf(
                    'Invalid Configuration in "%s". Both table and query cannot be set together.',
                    $table['name']
                ));
            }
        } else if (!isset($table['table'])) {
            throw new ConfigException(sprintf(
                'Invalid Configuration in "%s". One of table or query is required.',
                $table['name']
            ));
        } else if (!$this->isTableValid($table['table'])) {
            throw new ConfigException(sprintf(
                'Invalid Configuration in "%s". The table property requires "tableName" and "schema"',
                $table['name']
            ));
        }
    }

    private function validateParameters($parameters)
    {
        try {
            $processor = new Processor();
            $processedParameters = $processor->processConfiguration(
                $this->configDefinition,
                [$parameters]
            );

            if (isset($processedParameters['tables'])) {
                foreach ($processedParameters['tables'] as $table) {
                    $this->validateTableParameters($table);
                }
            } else {
                $this->validateTableParameters($processedParameters);
            }

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
        if (isset($this['parameters']['tables'])) {
            $tables = array_filter($this['parameters']['tables'], function ($table) {
                return ($table['enabled']);
            });
        } else {
            $tables = [$this['parameters']];
        }

        foreach ($tables as $table) {
            $imported[] = $this['extractor']->export($table);
        }

        return [
            'status' => 'success',
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
            'status' => 'success',
        ];
    }

    private function getTablesAction()
    {
        try {
            $output = [];
            $output['tables'] = $this['extractor']->getTables();
            $output['status'] = 'success';
        } catch (\Exception $e) {
            throw new UserException(sprintf("Failed to get tables: '%s'", $e->getMessage()), 0, $e);
        }
        return $output;
    }
}
