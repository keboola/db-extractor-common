<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Component\BaseComponent;
use Keboola\Component\UserException;
use Keboola\DbExtractor\Configuration\ActionConfigDefinition;
use Keboola\DbExtractor\Configuration\Config;
use Keboola\DbExtractor\Configuration\ConfigDefinition;
use Keboola\DbExtractor\Configuration\ConfigRowDefinition;
use Psr\Log\LoggerInterface;

class ExtractorAdapter extends BaseComponent
{
    /** @var BaseExtractor */
    private $extractor;

    /** @var string */
    private $action;

    /** @var bool */
    private $isConfigRow;

    /** @var array */
    private $parameters;

    // @todo - $action and $parameters might be accessed from parent class
    public function __construct(BaseExtractor $extractor, LoggerInterface $logger, string $action, array $parameters)
    {
        $this->action = $action;
        $this->isConfigRow = !isset($parameters['tables']);
        parent::__construct($logger);

        $this->extractor = $extractor;
        $this->parameters = $this->getConfig()->getParameters();
    }

    protected function getConfigDefinitionClass(): string
    {
        if ($this->action !== 'run') {
            return ActionConfigDefinition::class;
        } elseif (!$this->isConfigRow) {
            return ConfigDefinition::class;
        } else {
            return ConfigRowDefinition::class;
        }
    }

    protected function getConfigClass(): string
    {
        return Config::class;
    }

    public function run(): void
    {
        try {
            switch ($this->action) {
                case 'run':
                    $this->extractor->validateParameters($this->parameters);
                    $result = $this->runExtract();
                    break;
                case 'testConnection':
                    $result = $this->testConnection();
                    break;
                case 'getTables':
                    $result = $this->getTables();
                    break;
                default:
                    throw new UserException(sprintf('Undefined action "%s".', $this->action));
            }
        } catch (\Throwable $exception) {
            throw $exception;
        }

        print json_encode($result, JSON_PRETTY_PRINT);
    }

    public function runExtract(): array
    {
        return $this->extractor->extract($this->parameters);
    }

    public function testConnection(): array
    {
        $this->extractor->testConnection();

        return ['status' => 'success'];
    }

    public function getTables(): array
    {
        return [
            'tables' => $this->extractor->getTables(),
            'status' => 'success',
        ];
    }
}
