<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Exception\UserException;
use Pimple\Container;
use ErrorException;

class Application extends Container
{
    /** @var mixed|string $action */
    protected $action;

    /** @var mixed $parameters */
    protected $parameters;

    /** @var array $state */
    protected $state;

    /** @var Logger $logger */
    protected $logger;

    /** @var Extractor\Extractor $extractor */
    protected $extractor;

    public function __construct(array $config, Logger $logger, array $state = [])
    {
        static::setEnvironment();

        parent::__construct();

        $app = $this;

        $this['action'] = isset($config['action']) ? $config['action'] : 'run';

        $this->parameters = $config['parameters'];

        $this->state = $state;

        $this->logger = $logger;

        $extractorFactory = new ExtractorFactory($this->parameters, $this->state);
        $this->extractor = $extractorFactory->create($this->logger);
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
        $imported = [];
        $outputState = [];
        if (isset($this->parameters['tables'])) {
            $tables = (array) array_filter(
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
