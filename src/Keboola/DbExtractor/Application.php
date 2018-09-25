<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\Component\Logger;
use Keboola\DbExtractor\Configuration\ActionConfigRowDefinition;
use Keboola\DbExtractor\Configuration\ConfigDefinition;
use Keboola\DbExtractor\Configuration\ConfigRowDefinition;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\Extractor;
use Symfony\Component\Config\Definition\ConfigurationInterface;
use Symfony\Component\Config\Definition\Exception\Exception as ConfigException;
use Symfony\Component\Config\Definition\Processor;
use ErrorException;

class Application
{
    /** @var string */
    private $action;

    /** @var array */
    private $parameters;

    /** @var array */
    private $state;

    /** @var Logger */
    private $logger;

    /** @var Extractor */
    private $extractor;

    /** @var ConfigurationInterface */
    private $configDefinition;

    public function __construct(array $config, Logger $logger, array $state = [])
    {
        static::setEnvironment();

        $this->action = isset($config['action'])?$config['action']:'run';
        $this->parameters = $config['parameters'];
        $this->state = $state;
        $this->logger = $logger;

        $extractorFactory = new ExtractorFactory($this->parameters, $this->state);
        $this->extractor = $extractorFactory->create($this->logger);

        if (isset($this->parameters['tables'])) {
            $this->configDefinition = new ConfigDefinition();
        } else {
            if ($this->action === 'run') {
                $this->configDefinition = new ConfigRowDefinition();
            } else {
                $this->configDefinition = new ActionConfigRowDefinition();
            }
        }
    }

    public function run(): array
    {
        $this->parameters = $this->validateParameters($this->parameters);

        $actionMethod = $this->action . 'Action';
        if (!method_exists($this, $actionMethod)) {
            throw new UserException(sprintf("Action '%s' does not exist.", $this->action));
        }

        return $this->$actionMethod();
    }

    public function setConfigDefinition(ConfigurationInterface $definition): void
    {
        $this->configDefinition = $definition;
    }

    private function validateTableParameters(array $table): void
    {
        if (isset($table['incrementalFetchingColumn'])
            && $table['incrementalFetchingColumn'] !== "") {
            $this->extractor->validateIncrementalFetching(
                $table['table'],
                $table['incrementalFetchingColumn'],
                $table['incrementalFetchingLimit']?? null
            );
        }

        if (isset($table['incrementalFetching']['autoIncrementColumn']) && empty($table['primaryKey'])) {
            $this->logger->warn("An import autoIncrement column is being used but output primary key is not set.");
        }
    }

    private function validateParameters(array $parameters): array
    {
        try {
            $processor = new Processor();
            $processedParameters = $processor->processConfiguration(
                $this->configDefinition,
                [$parameters]
            );

            if ($this->action === 'run') {
                if (isset($processedParameters['tables'])) {
                    foreach ($processedParameters['tables'] as $table) {
                        $this->validateTableParameters($table);
                    }
                } else {
                    $this->validateTableParameters($processedParameters);
                }
            }

            return $processedParameters;
        } catch (ConfigException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }

    private function runAction(): array
    {
        $imported = [];
        $outputState = [];
        if (isset($this->parameters['tables'])) {
            $tables = array_filter(
                $this->parameters['tables'],
                function ($table) {
                    return ($table['enabled']);
                }
            );
            foreach ($tables as $table) {
                $exportResults = $this->extractor->export($table);
                $imported[] = $exportResults;
            }
        } else {
            $exportResults = $this->extractor->export($this->parameters);
            if (isset($exportResults['state'])) {
                $outputState = $exportResults['state'];
                unset($exportResults['state']);
            }
            $imported = $exportResults;
        }

        return [
            'status' => 'success',
            'imported' => $imported,
            'state' => $outputState,
        ];
    }

    private function testConnectionAction(): array
    {
        try {
            $this->extractor->testConnection();
        } catch (\Throwable $e) {
            throw new UserException(sprintf("Connection failed: '%s'", $e->getMessage()), 0, $e);
        }

        return [
            'status' => 'success',
        ];
    }

    private function getTablesAction(): array
    {
        try {
            $output = [];
            $output['tables'] = $this->extractor->getTables();
            $output['status'] = 'success';
        } catch (\Throwable $e) {
            throw new UserException(sprintf("Failed to get tables: '%s'", $e->getMessage()), 0, $e);
        }
        return $output;
    }

    public static function setEnvironment(): void
    {
        error_reporting(E_ALL);
        set_error_handler(function ($errno, $errstr, $errfile, $errline, array $errcontext): bool {
            if (!(error_reporting() & $errno)) {
                // respect error_reporting() level
                // libraries used in custom components may emit notices that cannot be fixed
                return false;
            }
            throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
        });
    }
}
