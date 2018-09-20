<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\ActionConfigRowDefinition;
use Keboola\DbExtractor\Configuration\ConfigRowDefinition;
use Keboola\DbExtractor\Exception\UserException;
use Pimple\Container;
use Symfony\Component\Config\Definition\ConfigurationInterface;
use Symfony\Component\Config\Definition\Exception\Exception as ConfigException;
use Symfony\Component\Config\Definition\Processor;
use ErrorException;

class Application extends Container
{
    /** @var string */
    protected $action;

    /** @var array */
    protected $parameters;

    /** @var array */
    protected $state;

    /** @var Logger */
    protected $logger;

    /** @var mixed */
    protected $extractor;

    /** @var ConfigurationInterface */
    private $configDefinition;

    public function __construct(array $config, Logger $logger, array $state = [])
    {
        static::setEnvironment();

        parent::__construct();

        $this->action = $config['action']?? 'run';
        $this->parameters = $config['parameters'];
        $this->state = $state;
        $this->logger = $logger;

        $extractorFactory = new ExtractorFactory($this->parameters, $this->state);
        $this->extractor = $extractorFactory->create($this->logger);

        if ($this->action === 'run') {
            $this->configDefinition = new ConfigRowDefinition();
        } else {
            $this->configDefinition = new ActionConfigRowDefinition();
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

    private function isTableValid(array $table): bool
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

    private function validateTableParameters(array $table): void
    {
        if (isset($table['query']) && $table['query'] !== '') {
            if (isset($table['table'])) {
                throw new ConfigException(
                    sprintf(
                        'Invalid Configuration for "%s". Both table and query cannot be set together.',
                        $table['outputTable']
                    )
                );
            }
            if (isset($table['incrementalFetchingColumn'])) {
                throw new ConfigException(
                    sprintf(
                        'Invalid Configuration for "%s". Incremental fetching is not supported for advanced queries.',
                        $table['outputTable']
                    )
                );
            }
        } else if (!isset($table['table'])) {
            throw new ConfigException(
                sprintf(
                    'Invalid Configuration for "%s". One of table or query is required.',
                    $table['outputTable']
                )
            );
        } else if (!$this->isTableValid($table['table'])) {
            throw new ConfigException(
                sprintf(
                    'Invalid Configuration for "%s". The table property requires "tableName" and "schema"',
                    $table['outputTable']
                )
            );
        } else if (isset($table['incrementalFetching']['autoIncrementColumn']) && empty($table['primaryKey'])) {
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
                $this->validateTableParameters($processedParameters);
            }

            return $processedParameters;
        } catch (ConfigException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }

    private function runAction(): array
    {
        $outputState = [];

        $exportResult = $this->extractor->export($this->parameters);
        if (isset($exportResult['state'])) {
            $outputState = $exportResult['state'];
            unset($exportResult['state']);
        }

        return [
            'status' => 'success',
            'imported' => $exportResult,
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
