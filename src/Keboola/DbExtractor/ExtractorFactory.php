<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Exception\UserException;

class ExtractorFactory
{
    private $parameters;

    private $state;

    public function __construct(array $parameters, array $state)
    {
        $this->parameters = $parameters;
        $this->state = $state;
    }

    public function create($logger)
    {
        $extractorClass = __NAMESPACE__ . '\\Extractor\\' . $this->parameters['extractor_class'];
        if (!class_exists($extractorClass)) {
            throw new UserException(sprintf("Extractor class '%s' doesn't exist", $extractorClass));
        }

        return new $extractorClass($this->parameters, $this->state, $logger);
    }
}
