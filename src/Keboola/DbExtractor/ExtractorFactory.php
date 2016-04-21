<?php

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/12/15
 * Time: 12:20
 */

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Exception\UserException;

class ExtractorFactory
{
    private $parameters;

    public function __construct($parameters)
    {
        $this->parameters = $parameters;
    }

    public function create($logger)
    {
        $extractorClass = __NAMESPACE__ . '\\Extractor\\' . $this->parameters['extractor_class'];
        if (!class_exists($extractorClass)) {
            throw new UserException(sprintf("Extractor class '%s' doesn't exist", $extractorClass));
        }

        return new $extractorClass($this->parameters, $logger);
    }
}
