<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\Component\Logger\AsyncActionLogging;
use Keboola\Component\Logger\SyncActionLogging;
use Keboola\DbExtractor\Exception\BadUsernameException;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\BaseExtractor;
use Keboola\DbExtractorConfig\Config;
use Keboola\DbExtractorConfig\Configuration\ActionConfigRowDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigRowDefinition;
use Pimple\Container;
use ErrorException;
use Psr\Log\LoggerInterface;

class Application extends Container
{
    protected Config $config;

    public function __construct(array $config, LoggerInterface $logger, array $state = [])
    {
        static::setEnvironment();

        parent::__construct();

        $app = $this;

        $this['action'] = isset($config['action']) ? $config['action'] : 'run';

        $this['parameters'] = $config['parameters'];

        $this['state'] = $state;

        $this['logger'] = $logger;

        // Setup logger, copied from php-component/src/BaseComponent.php
        // Will be removed in next refactoring steps,
        // ... when Application will be replace by standard BaseComponent
        if ($this['action'] !== 'run') { // $this->isSyncAction()
            if ($this['logger'] instanceof SyncActionLogging) {
                $this['logger']->setupSyncActionLogging();
            }
        } else {
            if ($this['logger']instanceof AsyncActionLogging) {
                $this['logger']->setupAsyncActionLogging();
            }
        }

        $this->buildConfig($config);

        $checkKbcRealuser = $config['image_parameters']['check_kbc_realuser'] ?? false;
        if ($checkKbcRealuser) {
            $this->checkUsername();
        }

        $this['extractor_factory'] = function () use ($app) {
            $configData = $app->config->getData();
            return new ExtractorFactory($configData['parameters'], $app['state']);
        };

        $this['extractor'] = function () use ($app) {
            return $app['extractor_factory']->create($app['logger']);
        };
    }

    public function run(): array
    {
        $actionMethod = $this['action'] . 'Action';
        if (!method_exists($this, $actionMethod)) {
            throw new UserException(sprintf('Action "%s" does not exist.', $this['action']));
        }

        return $this->$actionMethod();
    }

    protected function buildConfig(array $config): void
    {
        if ($this->isRowConfiguration($config)) {
            if ($this['action'] === 'run') {
                $this->config = new Config($config, new ConfigRowDefinition());
            } else {
                $this->config = new Config($config, new ActionConfigRowDefinition());
            }
        } else {
            $this->config = new Config($config, new ConfigDefinition());
        }
    }

    protected function createExportConfig(array $data): ExportConfig
    {
        return ExportConfig::fromArray($data);
    }

    protected function isRowConfiguration(array $config): bool
    {
        if (isset($config['parameters']['table']) || isset($config['parameters']['query'])) {
            return true;
        }

        if (!isset($config['parameters']['tables'])) {
            return true;
        }

        return false;
    }

    private function runAction(): array
    {
        $configData = $this->config->getData();
        $imported = [];
        $outputState = [];
        if (!$this->isRowConfiguration($configData)) {
            $tables = (array) array_filter(
                $configData['parameters']['tables'],
                function ($table) {
                    return ($table['enabled']);
                }
            );
            foreach ($tables as $table) {
                $exportResults = $this['extractor']->export($this->createExportConfig($table));
                $imported[] = $exportResults;
            }
        } else {
            $exportResults = $this['extractor']->export($this->createExportConfig($configData['parameters']));
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
            $this['extractor']->testConnection();
        } catch (\Throwable $e) {
            throw new UserException(sprintf("Connection failed: '%s'", $e->getMessage()), 0, $e);
        }

        return [
            'status' => 'success',
        ];
    }

    private function getTablesAction(): array
    {
        /** @var BaseExtractor $extractor */
        $extractor = $this['extractor'];
        $output = [];
        $output['tables'] = $extractor->getTables();
        $output['status'] = 'success';
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

    protected function checkUsername(): void
    {
        $realUsername = $this->getRealUsername();
        $dbUsername = $this->getDbUsername();

        if ($this->isTechnicalUsername($dbUsername)) {
            $this['logger']->info(sprintf(
                'Starting export data with a service account "%s".',
                $dbUsername
            ));
            return;
        }

        if ($realUsername !== $dbUsername) {
            throw new BadUsernameException(
                sprintf(
                    'Your username "%s" does not have permission to ' .
                    'run configuration with the database username "%s"',
                    $realUsername,
                    $dbUsername
                )
            );
        }

        $this['logger']->info(sprintf('Username "%s" has been verified.', $realUsername));
    }

    protected function isTechnicalUsername(string $username): bool
    {
        return substr($username, 0, 1) === '_';
    }

    protected function getDbUsername(): string
    {
        return $this->config->getData()['parameters']['db']['user'];
    }

    protected function getRealUsername(): string
    {
        return (string) getenv('KBC_REALUSER');
    }
}
