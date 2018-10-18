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
Create entrypoint script the `run.php` like this one in DataDir Tests:
https://github.com/keboola/db-extractor-common/blob/david-adapter/tests/FunctionalAdapter/run.php

The `$config` is loaded from `config.json` file. You have to set ENV variable `KBC_DATADIR` with path to your data folder (running component inside KBC provide it automatically). 
_We strongly recommend using [configuration rows schema](https://github.com/keboola/db-extractor-common/tree/master/tests/Old/data/common/exampleConfigRow.json)_

The extractor class must be child of [\Keboola\DbExtractorCommon\BaseExtractor](https://github.com/keboola/db-extractor-common/tree/master/src/Keboola/DbExtractor/Extractor/BaseExtractor.php) and implement all abstract methods:
 
- `extract(array $tables): array`
- `getTables(array $tables = []): array`
- `testConnection(): void`

If you want to implement incremental fetching, you must override:

- `validateIncrementalFetching(array $table, string $columnName, ?int $limit = null): void`

Also you can define custom Config Definition class:

- `getConfigDefinitionClass(): string`

Please check the existing implementations above for help getting started.
