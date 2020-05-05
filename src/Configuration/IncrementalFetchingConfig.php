<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\DbExtractor\Exception\InvalidArgumentException;
use Keboola\DbExtractor\Exception\PropertyNotSetException;
use Keboola\DbExtractor\ValueObject;

class IncrementalFetchingConfig implements ValueObject
{
    private string $column;

    private ?int $limit;

    public static function fromArray(array $data): ?self
    {
        // Enabled ?
        $enabled = $data['incremental'] ?? false;
        if (!is_bool($enabled)) {
            throw new InvalidArgumentException(sprintf(
                'Key "incremental" should be bool, given: %s',
                is_object($enabled) ? get_class($enabled) : gettype($enabled)
            ));
        } elseif ($enabled === false) {
            return null;
        }

        // Column
        if (!isset($data['incrementalFetchingColumn'])) {
            throw new InvalidArgumentException('Missing key "incrementalFetchingColumn".');
        }
        $column = $data['incrementalFetchingColumn'];

        // Limit
        $limit = $data['incrementalFetchingLimit'] ?? null;

        // Create object
        return new self($column, $limit);
    }

    public function __construct(string $column, ?int $limit)
    {
        if ($column === '') {
            throw new InvalidArgumentException('Column cannot be empty string.');
        }

        $this->column = $column;
        $this->limit = $limit;
    }

    public function getColumn(): string
    {
        return $this->column;
    }

    public function hasLimit(): bool
    {
        return $this->limit !== null;
    }

    public function getLimit(): int
    {
        if ($this->limit === null) {
            throw new PropertyNotSetException('Property "limit" is not set.');
        }

        return $this->limit;
    }
}
