# Database Extractor Common [DBC]
---

Common classes for creating vendor specific database extractors.

## Extractors using DBC
- [MySQL](https://github.com/keboola/db-extractor-mysql)
- [MSSQL](https://github.com/keboola/db-extractor-mssql)
- [PgSQL](https://github.com/keboola/db-extractor-pgsql)
- [Oracle](https://github.com/keboola/db-extractor-oracle)
- [Impala](https://github.com/keboola/db-extractor-impala)
- [Firebird](https://github.com/keboola/db-extractor-firebird)
- [DB2](https://github.com/keboola/db-extractor-db2)
- [Mongo DB](https://github.com/keboola/mongodb-extractor)

## Status
[![Build Status](https://travis-ci.org/keboola/db-extractor-common.svg)](https://travis-ci.org/keboola/db-extractor-common)

## Installation
Install via composer:

    php composer.phar require db-extractor-common

composer.json

    {
      "require": "db-extractor-common": ^2.0
    }
    
## Usage
Create entrypoint script file `run.php` like this one for Postgres extractor:

```php
<?php

use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Yaml\Yaml;

require_once(dirname(__FILE__) . "/vendor/keboola/db-extractor-common/bootstrap.php");

define('APP_NAME', 'ex-db-pgsql');

try {
    $arguments = getopt("d::", ["data::"]);
    if (!isset($arguments["data"])) {
        throw new UserException('Data folder not set.');
    }

    $config = Yaml::parse(file_get_contents($arguments["data"] . "/config.yml"));
    $config['data_dir'] = $arguments['data'];
    $config['extractor_class'] = 'PgSQL';

    $app = new Application($config);
    $app->run();

} catch(UserException $e) {

    $app['logger']->log('error', $e->getMessage(), (array) $e->getData());
    exit(1);

} catch(ApplicationException $e) {

    $app['logger']->log('error', $e->getMessage(), (array) $e->getData());
    exit($e->getCode() > 1 ? $e->getCode(): 2);

} catch(\Exception $e) {

    $app['logger']->log('error', $e->getMessage(), [
        'errFile' => $e->getFile(),
        'errLine' => $e->getLine(),
        'trace' => $e->getTrace()
    ]);
    exit(2);
}

$app['logger']->log('info', "Extractor finished successfully.");
exit(0);
```

You should define `APP_NAME` constant in format `ex-db-VENDOR`.
The $config is loaded from config.yml file, you have to provide values for `data_dir` and `extractor_class` keys.
`extractor_class` is the main class of derived extractor, it should extend `Keboola\DbExtractor\Extractor\Extractor`.
Often, the only thing you need to override, is the `createConnection()` method, here is an simple example taken from Impala extractor:

```php
<?php

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/12/15
 * Time: 13:03
 */

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Exception\UserException;

class Impala extends Extractor
{
    public function createConnection($params)
    {
        // convert errors to PDOExceptions
        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC
        ];

        // check params
        foreach (['host', 'database', 'user', 'password'] as $r) {
            if (!isset($params[$r])) {
                throw new UserException(sprintf("Parameter %s is missing.", $r));
            }
        }

        $port = isset($params['port']) ? $params['port'] : '21050';
        $dsn = sprintf(
            "odbc:DSN=MyImpala;HOST=%s;PORT=%s;Database=%s;UID=%s;PWD=%s;AuthMech=%s",
            $params['host'],
            $port,
            $params['database'],
            $params['user'],
            $params['password'],
            isset($params['auth_mech'])?$params['auth_mech']:3
        );

        $pdo = new \PDO($dsn, $params['user'], $params['password'], $options);
        $pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, true);

        return $pdo;
    }

    public function getConnection()
    {
        return $this->db;
    }
}

```
The `createConnection()` method returns an instance of \PDO, which is then used to connect and extract data from database as you can see in the extractor base class [https://github.com/keboola/db-extractor-common/blob/master/src/Keboola/DbExtractor/Extractor/Extractor.php](Extractor.php).

The namespace of your extractor class shoud be `Keboola\DbExtractor\Extractor` and the name of the class should corespond to DB vendor name i.e. PgSQL, Oracle, Impala, Firebrid, DB2 and so on.

## Testing
Directory structure: (example from Impala Extractor)

      src
      tests
      +-- data
          +-- impala
              +-- config.yml
          Keboola
          +-- Extractor
              +-- ImpalaTest.php


`config.yml` file is a configuration file for test run.
`ImpalaTest.php` holds actual phpunit tests, in this case, it's very simple:

```php
<?php

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/12/15
 * Time: 14:25
 */

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Test\ExtractorTest;

class ImpalaTest extends ExtractorTest
{
    /** @var Application */
    protected $app;

    public function setUp()
    {
        define('APP_NAME', 'ex-db-impala');
        $this->app = new Application($this->getConfig('impala'));
    }

    public function testRun()
    {
        $result = $this->app->run();

        $this->assertEquals('ok', $result['status']);

        $this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv');
        $this->assertFileExists($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest');
    }

}
```
