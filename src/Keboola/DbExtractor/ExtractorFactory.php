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

    private $driversMap = [
        'common'    => 'Common',
        'impala'    => 'Impala',
        'mysql'     => 'MySQL',
        'oracle'    => 'Oracle',
        'pgsql'     => 'PgSQL',
        'mssql'     => 'MSSQL',
        'redshift'  => 'Redshift',
        'firebird'  => 'Firebird'
    ];

    public function __construct($config)
    {
        $this->config = $config;
    }

    public function create($logger)
    {
        $driver = $this->config['parameters']['db']['driver'];
        if (!array_key_exists($driver, $this->driversMap)) {
            throw new UserException(sprintf("Driver '%s' is not supported", $driver));
        }

        $className = __NAMESPACE__ . '\\Extractor\\' . $this->driversMap[$driver];

        return new $className($this->config, $logger);
    }
}
