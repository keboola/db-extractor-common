<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\DbExtractor\Exception\InvalidArgumentException;
use Keboola\DbExtractor\Exception\PropertyNotSetException;
use Keboola\DbExtractor\ValueObject;

class ExportConfig implements ValueObject
{
    public const DEFAULT_MAX_TRIES = 5;

    /** Custom export query */
    private ?string $query;

    /** Table that will be exported (if query is not set) */
    private ?InputTable $table;

    /** Configuration of incremental fetching */
    private ?IncrementalFetchingConfig $incrementalFetchingConfig;

    /** Columns that will be exported, cannot be used with query, if null => all columns */
    private ?array $columns;

    /** Name of the output table */
    private string $outputTable;

    /** PK key can be composed of multiple columns, therefore an array */
    private ?array $primaryKey;

    /** Number of max retries if an error occurs */
    private int $maxRetries;

    public static function fromArray(array $data): self
    {
        if (!isset($data['outputTable'])) {
            throw new InvalidArgumentException('Key "outputTable" is required.');
        }

        if (!isset($data['table']) && !isset($data['query'])) {
            throw new InvalidArgumentException('Key "table" or "query" must be set.');
        }

        // Compatibility fix: unset columns when custom query is set
        if (!empty($data['query']) && !empty($data['columns'])) {
            unset($data['columns']);
        }

        $query = empty($data['query']) ? null : $data['query']; // undefined/empty string => null
        $table = $query ? null : InputTable::fromArray($data);
        $incrementalFetchingConfig = IncrementalFetchingConfig::fromArray($data);
        $columns = empty($data['columns']) ? null : $data['columns']; // undefined/empty array => null
        $outputTable = $data['outputTable'];
        $primaryKey = empty($data['primaryKey']) ? null : $data['primaryKey']; // undefined/empty array => null
        $maxRetries = $data['retries'] ?? self::DEFAULT_MAX_TRIES;

        return new self($query, $table, $incrementalFetchingConfig, $columns, $outputTable, $primaryKey, $maxRetries);
    }

    public function __construct(
        ?string $query,
        ?InputTable $table,
        ?IncrementalFetchingConfig $incrementalFetchingConfig,
        ?array $columns,
        string $outputTable,
        ?array $primaryKey,
        int $maxRetries
    ) {
        if ($query === null && $table === null) {
            throw new InvalidArgumentException('Query or table must be specified.');
        }

        if ($query === '') {
            throw new InvalidArgumentException('Query cannot be empty string.');
        }

        if ($outputTable === '') {
            throw new InvalidArgumentException('Output table cannot be empty string.');
        }

        if ($maxRetries < 0) {
            throw new InvalidArgumentException('Max retries must be >= 0.');
        }

        if ($columns && $query) {
            throw new InvalidArgumentException('Columns cannot be used with custom query.');
        }

        if (is_array($columns) && count($columns) === 0) {
            throw new InvalidArgumentException('Columns cannot be empty array, null expected.');
        }

        if (is_array($primaryKey) && count($primaryKey) === 0) {
            throw new InvalidArgumentException('Primary key cannot be empty array, null expected.');
        }

        $this->query = $query;
        $this->table = $table;
        $this->incrementalFetchingConfig = $incrementalFetchingConfig;
        $this->columns = $columns;
        $this->outputTable = $outputTable;
        $this->primaryKey = $primaryKey;
        $this->maxRetries = $maxRetries;
    }

    public function hasTable(): bool
    {
        return $this->table !== null;
    }

    public function getTable(): InputTable
    {
        if ($this->table === null) {
            throw new PropertyNotSetException('Table is not set, use "query".');
        }

        return $this->table;
    }

    public function isIncremental(): bool
    {
        return $this->incrementalFetchingConfig !== null;
    }

    public function hasIncrementalLimit(): bool
    {
        if ($this->incrementalFetchingConfig === null) {
            return false;
        }

        return $this->incrementalFetchingConfig->hasLimit();
    }

    public function getIncrementalLimit(): int
    {
        if ($this->incrementalFetchingConfig === null) {
            throw new PropertyNotSetException('Incremental fetching is not enabled.');
        }

        return $this->incrementalFetchingConfig->getLimit();
    }

    public function getIncrementalColumn(): string
    {
        if ($this->incrementalFetchingConfig === null) {
            throw new PropertyNotSetException('Incremental fetching is not enabled.');
        }

        return $this->incrementalFetchingConfig->getColumn();
    }

    public function getIncrementalFetchingConfig(): IncrementalFetchingConfig
    {
        if ($this->incrementalFetchingConfig === null) {
            throw new PropertyNotSetException('Incremental fetching is not enabled.');
        }

        return $this->incrementalFetchingConfig;
    }

    public function hasQuery(): bool
    {
        return $this->query !== null;
    }

    public function getQuery(): string
    {
        if ($this->query === null) {
            throw new PropertyNotSetException('Query is not set.');
        }

        return $this->query;
    }

    public function hasColumns(): bool
    {
        return $this->columns !== null;
    }

    public function getColumns(): array
    {
        if ($this->columns === null) {
            throw new PropertyNotSetException('Columns are not set.');
        }

        return $this->columns;
    }

    public function getOutputTable(): string
    {
        return $this->outputTable;
    }

    public function hasPrimaryKey(): bool
    {
        return $this->primaryKey !== null;
    }

    public function getPrimaryKey(): array
    {
        if ($this->primaryKey === null) {
            throw new PropertyNotSetException('Primary key is not set.');
        }

        return $this->primaryKey;
    }

    public function getMaxRetries(): int
    {
        return $this->maxRetries;
    }
}
