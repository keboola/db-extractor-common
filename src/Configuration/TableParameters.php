<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Configuration;

class TableParameters
{
    /** @var string|null */
    private $query;

    /** @var array|null */
    private $columns;

    /** @var string */
    private $outputTable;

    /** @var TableDetailParameters|null */
    private $tableDetail;

    /** @var bool */
    private $incremental;

    /** @var string|null */
    private $incrementalFetchingColumn;

    /** @var int|null */
    private $incrementalFetchingLimit;

    /** @var bool */
    private $enabled;

    /** @var array|null */
    private $primaryKey;

    /** @var int|null */
    private $retries;

    public function __construct(array $table)
    {
        $this->query = $table['query'] ?? null;
        $this->columns = $table['columns'] ?? null;
        $this->outputTable = $table['outputTable'];
        $this->tableDetail = isset($table['table']) ? TableDetailParameters::fromRaw($table['table']) : null;
        $this->incremental = $table['incremental'] ?? false;
        $this->incrementalFetchingColumn = $table['incrementalFetchingColumn'] ?? null;
        $this->incrementalFetchingLimit = $table['incrementalFetchingLimit'] ?? null;
        $this->enabled = $table['enabled'] ?? true;
        $this->primaryKey = $table['primaryKey'] ?? null;
        $this->retries = $table['retries'] ?? null;
    }

    public function getQuery(): ?string
    {
        return $this->query;
    }

    public function getColumns(): ?array
    {
        return $this->columns;
    }

    public function getOutputTable(): string
    {
        return $this->outputTable;
    }

    public function getTableDetail(): ?TableDetailParameters
    {
        return $this->tableDetail;
    }

    public function isIncremental(): bool
    {
        return $this->incremental;
    }

    public function getIncrementalFetchingColumn(): ?string
    {
        return $this->incrementalFetchingColumn;
    }

    public function getIncrementalFetchingLimit(): ?int
    {
        return (int) $this->incrementalFetchingLimit;
    }

    public function isEnabled(): bool
    {
        return $this->enabled;
    }

    public function getPrimaryKey(): ?array
    {
        return $this->primaryKey;
    }

    public function getRetries(): ?int
    {
        return (int) $this->retries;
    }


    public static function fromRaw(array $table): TableParameters
    {
        return new TableParameters($table);
    }
}
