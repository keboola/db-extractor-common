<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

class ConfigDefinitionValidationHelper
{
    public const MESSAGE_TABLE_OR_QUERY_MUST_BE_DEFINED = 'Either "table" or "query" must be defined.';
    public const MESSAGE_TABLE_AND_QUERY_CANNOT_BE_SET_TOGETHER = 'Both "table" and "query" cannot be set together.';
    public const MESSAGE_CUSTOM_QUERY_CANNOT_BE_FETCHED_INCREMENTALLY
        = 'Incremental fetching is not supported for advanced queries.';


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
