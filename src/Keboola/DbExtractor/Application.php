<?php

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/12/15
 * Time: 12:17
 */

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Exception\UserException;
use Pimple\Container;

class Application extends Container
{
    public function __construct($config)
    {
        parent::__construct();

        $app = $this;

        $this['config'] = $config;

        $this['logger'] = function() use ($app) {
            return new Logger(APP_NAME);
        };

        $this['extractor_factory'] = function() use ($app) {
            return new ExtractorFactory($app['config']);
        };

        $this['extractor'] = function() use ($app) {
            return $app['extractor_factory']->create($app['logger']);
        };
    }

    public function run()
    {
        if (empty($this['config']['parameters']['tables'])) {
            throw new UserException("No tables defined for extraction. Check your configuration for key 'tables'.");
        }

        $imported = [];
        $tables = array_filter($this['config']['parameters']['tables'], function ($table) {
            return ($table['enabled']);
        });

        foreach ($tables as $table) {
            $imported[] = $this['extractor']->export($table);
        }

        return [
            'status' => 'ok',
            'imported' => $imported
        ];
    }

}