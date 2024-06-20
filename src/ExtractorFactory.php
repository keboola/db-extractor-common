<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\Component\Config\DatatypeSupport;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\BaseExtractor;
use Psr\Log\LoggerInterface;

class ExtractorFactory
{
    private array $parameters;

    private array $state;

    public function __construct(array $parameters, array $state)
    {
        $this->parameters = $parameters;
        $this->state = $state;
    }

    public function create(LoggerInterface $logger, string $action, DatatypeSupport $datatypeSupport): BaseExtractor
    {
        $extractorClass = __NAMESPACE__ . '\\Extractor\\' . $this->parameters['extractor_class'];
        if (!class_exists($extractorClass)) {
            throw new UserException(sprintf("Extractor class '%s' doesn't exist", $extractorClass));
        }

        return new $extractorClass($this->parameters, $this->state, $logger, $action, $datatypeSupport);
    }
}
