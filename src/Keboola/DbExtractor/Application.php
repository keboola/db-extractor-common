<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\Extractor;
use Keboola\DbExtractorConfig\Config;
use Keboola\DbExtractorConfig\Configuration\ActionConfigRowDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigRowDefinition;
use Keboola\DbExtractorLogger\Logger;
use Pimple\Container;
use ErrorException;

class Application extends Container
{
    /** @var Config $config */
    protected $config;

    /** @var Extractor $extractor */
    protected $extractor;

    /** @var Logger $logger */
    protected $logger;

    /** @var string $action */
    protected $action;

    /** @var array $state */
    protected $state;

    public function __construct(array $config, Logger $logger, array $state = [])
    {
        static::setEnvironment();

        parent::__construct();

        $this->action = isset($config['action']) ? (string) $config['action'] : 'run';

        $this->state = $state;

        $this->logger = $logger;

        $extractorFactory = new ExtractorFactory($config['parameters'], $this->state);
        $this->extractor = function () use ($extractorFactory, $logger) {
            return $extractorFactory->create($logger);
        };

        if (isset($config['parameters']['tables'])) {
            $this->config = new Config($config, new ConfigDefinition());
        } else {
            if ($this->action === 'run') {
                $this->config = new Config($config, new ConfigRowDefinition());
            } else {
                $this->config = new Config($config, new ActionConfigRowDefinition());
            }
        }
    }

    public function run(): array
    {
        $actionMethod = $this->action . 'Action';
        if (!method_exists($this, $actionMethod)) {
            throw new UserException(sprintf('Action "%s" does not exist.', $this->action));
        }

        return $this->$actionMethod();
    }

    private function runAction(): array
    {
        $configData = $this->config->getData();
        $imported = [];
        $outputState = [];
        if (isset($configData['parameters']['tables'])) {
            $tables = (array) array_filter(
                $configData['parameters']['tables'],
                function ($table) {
                    return ($table['enabled']);
                }
            );
            foreach ($tables as $table) {
                $exportResults = $this->extractor->export($table);
                $imported[] = $exportResults;
            }
        } else {
            $exportResults = $this->extractor->export($configData['parameters']);
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
