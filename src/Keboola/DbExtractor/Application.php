<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\Component\BaseComponent;
use Keboola\DbExtractor\Configuration\ActionConfigDefinition;
use Keboola\DbExtractor\Configuration\Config;
use Keboola\DbExtractor\Configuration\ConfigDefinition;
use Keboola\DbExtractor\Configuration\ConfigRowDefinition;
use Keboola\Component\UserException;
use Keboola\DbExtractor\Extractor\Extractor;
use Psr\Log\LoggerInterface;
use Symfony\Component\Config\Definition\ConfigurationInterface;
use Symfony\Component\Config\Definition\Exception\Exception as ConfigException;

class Application extends BaseComponent
{
    /** @var array */
    private $state;

    /** @var Extractor */
    private $extractor;

    /** @var ConfigurationInterface */
    private $configDefinition;

    /** @var string */
    private $configDefinitionClassName;

    public function __construct(array $config, LoggerInterface $logger, array $state = [])
    {
        putenv(sprintf('KBC_DATADIR=%s', $config['parameters']['data_dir']));
        $this->setConfigDefinitionClassNameByConfig($config);

        parent::__construct($logger);

        $this->state = $state;

        $extractorFactory = new ExtractorFactory($this->getConfig()->getParameters(), $this->state);
        $this->extractor = $extractorFactory->create($this->getLogger());
    }

    private function setConfigDefinitionClassNameByConfig(array $config): void
    {
        if ($this->getConfigDefinition()) {
            $this->configDefinitionClassName = get_class($this->getConfigDefinition());
        } elseif ($config['action'] !== 'run') {
            $this->configDefinitionClassName = ActionConfigDefinition::class;
        } elseif (isset($config['parameters']['tables'])) {
            $this->configDefinitionClassName = ConfigDefinition::class;
        } else {
            $this->configDefinitionClassName = ConfigRowDefinition::class;
        }
    }

    protected function getConfigClass(): string
    {
        return Config::class;
    }

    protected function getConfigDefinitionClass(): string
    {
        return $this->configDefinitionClassName;
    }

    public function run(): void
    {
        $this->validateParameters($this->getConfig()->getParameters());

        $actionMethod = $this->getConfig()->getAction() . 'Action';
        if (!method_exists($this, $actionMethod)) {
            throw new UserException(sprintf("Action '%s' does not exist.", $this->getConfig()->getAction()));
        }

        $this->$actionMethod();
    }

    public function setConfigDefinition(ConfigurationInterface $definition): void
    {
        $this->configDefinition = $definition;
    }

    public function getConfigDefinition(): ?ConfigurationInterface
    {
        return $this->configDefinition;
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
            $this->getLogger()->warning(
                "An import autoIncrement column is being used but output primary key is not set."
            );
        }
    }

    private function validateParameters(array $parameters): void
    {
        try {
            if ($this->getConfig()->getAction() === 'run') {
                if (isset($parameters['tables'])) {
                    foreach ($parameters['tables'] as $table) {
                        $this->validateTableParameters($table);
                    }
                } else {
                    $this->validateTableParameters($parameters);
                }
            }
        } catch (ConfigException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }

    private function runAction(): void
    {
        $imported = [];
        $outputState = [];
        if (isset($this->getConfig()->getParameters()['tables'])) {
            $tables = array_filter(
                $this->getConfig()->getParameters()['tables'],
                function ($table) {
                    return ($table['enabled']);
                }
            );
            foreach ($tables as $table) {
                $exportResults = $this->extractor->export($table);
                $imported[] = $exportResults;
            }
        } else {
            $exportResults = $this->extractor->export($this->getConfig()->getParameters());
            if (isset($exportResults['state'])) {
                $outputState = $exportResults['state'];
                unset($exportResults['state']);
            }
            $imported = $exportResults;
        }

        print json_encode(
            [
                'status' => 'success',
                'imported' => $imported,
                'state' => $outputState,
            ],
            JSON_PRETTY_PRINT
        );
    }

    private function testConnectionAction(): void
    {
        try {
            $this->extractor->testConnection();
        } catch (\Throwable $e) {
            throw new UserException(sprintf("Connection failed: '%s'", $e->getMessage()), 0, $e);
        }

        print json_encode(
            ['status' => 'success'],
            JSON_PRETTY_PRINT
        );
    }

    private function getTablesAction(): void
    {
        try {
            $output = [];
            $output['tables'] = $this->extractor->getTables();
            $output['status'] = 'success';
        } catch (\Throwable $e) {
            throw new UserException(sprintf("Failed to get tables: '%s'", $e->getMessage()), 0, $e);
        }

        print json_encode(
            $output,
            JSON_PRETTY_PRINT
        );
    }
}
