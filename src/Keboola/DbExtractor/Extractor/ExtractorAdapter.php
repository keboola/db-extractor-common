<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Component\UserException;

class ExtractorAdapter
{
    /** @var BaseExtractor */
    private $extractor;

    public function __construct(BaseExtractor $extractor)
    {
        $this->extractor = $extractor;
    }

    public function run(): void
    {
        $action = $this->extractor->getConfig()->getAction();
        $parameters = $this->extractor->getConfig()->getParameters();
        try {
            switch ($action) {
                case 'run':
                    $this->extractor->validateParameters($parameters);
                    $result = $this->runExtract($parameters); // @todo - save state into state file
                    break;
                case 'testConnection':
                    $result = $this->testConnection();
                    break;
                case 'getTables':
                    $result = $this->getTables();
                    break;
                default:
                    throw new UserException(sprintf('Undefined action "%s".', $action));
            }
        } catch (\Throwable $exception) {
            throw $exception;
        }

        print json_encode($result, JSON_PRETTY_PRINT);
    }

    private function getTables(): array
    {
        return [
            'tables' => $this->extractor->getTables(),
            'status' => 'success',
        ];
    }

    private function runExtract(array $parameters): array
    {
        return $this->extractor->extract($parameters);
    }

    private function testConnection(): array
    {
        $this->extractor->testConnection();

        return ['status' => 'success'];
    }
}
