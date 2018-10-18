<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Configuration;

use Keboola\DbExtractorCommon\BaseExtractor;

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

    /** @var int */
    private $retries;

    public function __construct(
        ?string $query = null,
        ?array $columns = null,
        ?string $outputTable = null,
        ?TableDetailParameters $tableDetailParameters = null,
        bool $incremental = false,
        ?string $incrementalFetchingColumn = null,
        ?int $incrementalFetchingLimit = null,
        bool $enabled = true,
        ?array $primaryKey = null,
        int $retries = BaseExtractor::DEFAULT_MAX_TRIES
    ) {
        $this->query = $query;
        $this->columns = $columns;
        $this->outputTable = $outputTable;
        $this->tableDetail = $tableDetailParameters;
        $this->incremental = $incremental;
        $this->incrementalFetchingColumn = $incrementalFetchingColumn;
        $this->incrementalFetchingLimit = $incrementalFetchingLimit;
        $this->enabled = $enabled;
        $this->primaryKey = $primaryKey;
        $this->retries = $retries;
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

    public function getRetries(): int
    {
        return $this->retries;
    }


    public static function fromRaw(array $table): TableParameters
    {
        return new TableParameters(
            $table['query'] ?? null,
            $table['columns'] ?? null,
            $table['outputTable'] ?? null,
            isset($table['table']) ? TableDetailParameters::fromRaw($table['table']) : null,
            $table['incremental'] ?? false,
            $table['incrementalFetchingColumn'] ?? null,
            isset($table['incrementalFetchingLimit']) ? (int) $table['incrementalFetchingLimit'] : null,
            $table['enabled'] ?? true,
            $table['primaryKey'] ?? null,
            $table['retries']
        );
    }
}
