# Database Extractor Common [DBC]
---

Common classes for creating vendor specific database extractors.

## Extractors using DBC
- [MySQL](https://github.com/keboola/db-extractor-mysql)
- [MSSQL](https://github.com/keboola/db-extractor-mssql)
- [PgSQL](https://github.com/keboola/db-extractor-pgsql)
- [Oracle](https://github.com/keboola/db-extractor-oracle) (private repository)
- [Impala](https://github.com/keboola/db-extractor-impala) 
- [Firebird](https://github.com/keboola/db-extractor-firebird)
- [DB2](https://github.com/keboola/db-extractor-db2)
- [Redshift](https://github.com/keboola/db-extractor-redshift)
- [Snowflake](https://github.com/keboola/db-extractor-snowflake)

## Development and running tests

    docker compose build
    docker compose run --rm tests  # runs the tests

## Usage
Add the library to your component's composer:

    php composer.phar require db-extractor-common

composer.json

    {
      "require": "db-extractor-common": ^8.0
    }
    
## Usage
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

## Check username

Check username functionality compares 
    - value of the `KBC_REALUSER` environment variable 
    - and username of the database user from the configuration.

If enabled, values must be same to run the component.

If a service account is used, check is skipped. For more information see `UsernameChecker` class.

**Example image / stack parameters**
```json
{
  "checkUsername": {
    "enabled": true
  }
}
```

Service account can be defined by regular expression:
```json
{
  "checkUsername": {
    "enabled": true,
    "serviceAccountRegexp": "~^service_~i"
  }
}
```

Or user account can be defined by regular expression (inverted logic):
```json
{
  "checkUsername": {
    "enabled": true,
    "userAccountRegexp": "~^[0-9]{3}_"
  }
}
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
