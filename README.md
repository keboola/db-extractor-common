# Database Extractor Common [DBC]
---
[![Build Status](https://travis-ci.org/keboola/db-extractor-common.svg?branch=master)](https://travis-ci.org/keboola/db-extractor-common)

Common classes for creating vendor specific database extractors.

## Extractors using DBC
- [MySQL](https://github.com/keboola/db-extractor-mysql)
- [MSSQL](https://github.com/keboola/db-extractor-mssql)
- [PgSQL](https://github.com/keboola/db-extractor-pgsql)
- [Oracle](https://github.com/keboola/db-extractor-oracle) (private repository)
- [Impala](https://github.com/keboola/db-extractor-impala) 
- [Firebird](https://github.com/keboola/db-extractor-firebird)
- [DB2](https://github.com/keboola/db-extractor-db2)

## Development and running tests

    docker-compose build
    docker-compose run --rm tests  # runs the tests

## Usage
Add the library to your component's composer:

    php composer.phar require db-extractor-common

composer.json

    {
      "require": "db-extractor-common": ^10.0
    }

### Version 10
_Version 10 changes a way of creating an instance of extractors. Old usage is still available but it is marked as deprecated and will be remove in next major version._

Create entrypoint script the `run.php` like this one in DataDir Tests:
https://github.com/keboola/db-extractor-common/blob/david-adapter/tests/FunctionalAdapter/run.php

The `$config` is loaded from `config.json` file. You have to set ENV variable `KBC_DATADIR` with path to your data folder (running component inside KBC provide it automatically). 
_We strongly recommend using [configuration rows schema](https://github.com/keboola/db-extractor-common/blob/david-adapter/tests/Old/data/common/exampleConfigRow.json)_

The extractor class must be child of [\Keboola\DbExtractor\Extractor\BaseExtractor](https://github.com/keboola/db-extractor-common/blob/david-adapter/src/Keboola/DbExtractor/Extractor/BaseExtractor.php) and implement all abstract methods:
 
- `extract(array $tables): array`
- `getTables(array $tables = []): array`
- `testConnection(): void`

If you want to implement incremental fetching, you must override: 
- `validateIncrementalFetching(array $table, string $columnName, ?int $limit = null): void`

Then you can pass the extractor class as a first constructor's argument to [\Keboola\DbExtractor\Extractor\ExtractorAdapter](https://github.com/keboola/db-extractor-common/blob/david-adapter/src/Keboola/DbExtractor/Extractor/ExtractorAdapter.php)

On created instance of `ExtractorAdapter` you can simply call method `run()` and that's it!

### Version 9 and older _[deprecated]_
Create entrypoint script file `run.php` like this one for Mysql extractor:
https://github.com/keboola/db-extractor-mysql/blob/master/src/run.php

Note that as of version 7, configuration rows are supported so it is not necessary to support .yml configs or table array configurations.

The $config is loaded from the `config.json` file.  You have to provide values for the `data_dir` and `extractor_class` keys.
`extractor_class` is the main class of derived extractor, it should extend `Keboola\DbExtractor\Extractor\Extractor`.

You will need to implement the following methods: 
- `createConnection(array $params)` 
- `testConnection()`
- `simpleQuery(array $table, array $columns = array()): string`
- `getTables(?array $tables = null): array;`

Note that to support identifier sanitation, the getTables method should return a `sanitizedName` property for each 
column.  This `sanitizedName` should be created using `Keboola\php-utils\sanitizeColumnName`

If you want to implement incremental fetching, you must implement   
`validateIncrementalFetching(array $table, string $columnName, ?int $limit = null): void`  
Please see the sample in the `Common` class: https://github.com/keboola/db-extractor-common/blob/master/src/Keboola/DbExtractor/Extractor/Common.php#L52 

The namespace of your extractor class shoud be `Keboola\DbExtractor\Extractor` and the name of the class should corespond to DB vendor name i.e. PgSQL, Oracle, Impala, Firebrid, DB2 and so on.

Please check the existing implementations above for help getting started.
