<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\Component\BaseComponent;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Configuration\ActionConfigRowDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigRowDefinition;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;
use Psr\Log\LoggerInterface;
use Throwable;

class Application extends BaseComponent
{
    public function __construct(LoggerInterface $logger)
    {
        parent::__construct($logger);
        $this->checkUsername();
    }

    protected function run(): void
    {
        $extractorFactory = new ExtractorFactory(
            $this->getConfig()->getParameters(),
            $this->getInputState(),
        );

        $extractor = $extractorFactory->create(
            $this->getLogger(),
            $this->getConfig()->getAction(),
            $this->getConfig()->getDataTypeSupport(),
        );

        if (!$this->isRowConfiguration($this->getConfig()->getData())) {
            $tables = array_filter(
                $this->getConfig()->getParameters()['tables'],
                function ($table) {
                    return ($table['enabled']);
                },
            );
            foreach ($tables as $table) {
                $extractor->export($this->createExportConfig($table));
            }
        } else {
            $exportResults = $extractor->export($this->createExportConfig($this->getConfig()->getParameters()));
            if (isset($exportResults['state'])) {
                $this->writeOutputStateToFile($exportResults['state']);
            }
        }
    }

    protected function getConfigDefinitionClass(): string
    {
        if ($this->isRowConfiguration($this->getRawConfig())) {
            $action = $this->getRawConfig()['action'] ?? 'run';
            if ($action === 'run') {
                return ConfigRowDefinition::class;
            } else {
                return ActionConfigRowDefinition::class;
            }
        }

        return ConfigDefinition::class;
    }

    protected function createExportConfig(array $data): ExportConfig
    {
        return ExportConfig::fromArray($data);
    }

    protected function isRowConfiguration(array $config): bool
    {
        $parameters = $config['parameters'];
        if (isset($parameters['table']) || isset($parameters['query'])) {
            return true;
        }

        if (!isset($parameters['tables'])) {
            return true;
        }

        return false;
    }

    protected function checkUsername(): void
    {
        $usernameChecker = new UsernameChecker($this->getLogger(), $this->getConfig());
        $usernameChecker->checkUsername();
    }

    protected function getSyncActions(): array
    {
        return [
            'getTables' => 'getTablesAction',
            'testConnection' => 'testConnectionAction',
        ];
    }

    protected function testConnectionAction(): array
    {
        $extractorFactory = new ExtractorFactory(
            $this->getConfig()->getParameters(),
            $this->getInputState(),
        );

        $extractor = $extractorFactory->create(
            $this->getLogger(),
            $this->getConfig()->getAction(),
            $this->getConfig()->getDataTypeSupport(),
        );

        try {
            $extractor->testConnection();
        } catch (Throwable $e) {
            throw new UserException(sprintf("Connection failed: '%s'", $e->getMessage()), 0, $e);
        }

        return [
            'status' => 'success',
        ];
    }

    protected function getTablesAction(): array
    {
        $extractorFactory = new ExtractorFactory(
            $this->getConfig()->getParameters(),
            $this->getInputState(),
        );

        $extractor = $extractorFactory->create(
            $this->getLogger(),
            $this->getConfig()->getAction(),
            $this->getConfig()->getDataTypeSupport(),
        );

        $output = [
            'tables' => $extractor->getTables(),
            'status' => 'success',
        ];
        return $output;
    }
}
