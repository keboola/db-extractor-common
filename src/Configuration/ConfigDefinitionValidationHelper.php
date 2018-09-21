<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Configuration;

class ConfigDefinitionValidationHelper
{
    public const MESSAGE_DATABASE_AND_TABLE_SCHEMA_ARE_DIFFERENT = 'Table schema and database mismatch.';
    public const MESSAGE_TABLE_OR_QUERY_MUST_BE_DEFINED = 'Either "table" or "query" must be defined.';
    public const MESSAGE_TABLE_AND_QUERY_CANNOT_BE_SET_TOGETHER = 'Both "table" and "query" cannot be set together.';
    public const MESSAGE_CUSTOM_QUERY_CANNOT_BE_FETCHED_INCREMENTALLY
        = 'Incremental fetching is not supported for advanced queries.';


    public static function isDatabaseAndTableSchemaEqual(array $table, ?string $database = null): bool
    {
        if (!$database || !isset($table['table'])) {
            return false;
        }

        return $table['table']['schema'] !== $database;
    }

    public static function isNeitherQueryNorTableDefined(array $table): bool
    {
        return !isset($table['query']) && !isset($table['table']);
    }

    public static function areBothQueryAndTableSet(array $table): bool
    {
        return isset($table['query']) && isset($table['table']);
    }

    public static function isIncrementalFetchingSetForAdvancedQuery(array $table): bool
    {
        return isset($table['query']) && isset($table['incrementalFetchingColumn']);
    }
}
