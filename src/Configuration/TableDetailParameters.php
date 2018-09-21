<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Configuration;

class TableDetailParameters
{
    /** @var string */
    private $schema;

    /** @var string */
    private $tableName;

    public function __construct(string $schema, string $tableName)
    {
        $this->schema = $schema;
        $this->tableName = $tableName;
    }

    public function getSchema(): string
    {
        return $this->schema;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }

    public static function fromRaw(array $tableDetail): TableDetailParameters
    {
        return new TableDetailParameters(
            $tableDetail['schema'],
            $tableDetail['tableName']
        );
    }
}
