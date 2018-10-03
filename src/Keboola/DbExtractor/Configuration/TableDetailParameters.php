<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

class TableDetailParameters
{
    /** @var string */
    private $schema;

    /** @var string */
    private $tableName;

    public function __construct(array $tableDetail)
    {
        $this->schema = $tableDetail['schema'];
        $this->tableName = $tableDetail['tableName'];
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
        return new TableDetailParameters($tableDetail);
    }
}
