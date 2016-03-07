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
    private $config;

    public function __construct($config)
    {
        $this->config = $config;
    }

    public function create($logger)
    {
        $extractorClass = __NAMESPACE__ . '\\Extractor\\' . $this->config['extractor_class'];
        if (!class_exists($extractorClass)) {
            throw new UserException(sprintf("Extractor class '%s' doesn't exist", $extractorClass));
        }

        return new $extractorClass($this->config, $logger);
    }
}
