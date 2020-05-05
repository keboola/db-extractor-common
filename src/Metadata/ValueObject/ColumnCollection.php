<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Metadata\ValueObject;

use Countable;
use Iterator;
use IteratorAggregate;
use ArrayIterator;
use Keboola\DbExtractor\Exception\InvalidArgumentException;
use Keboola\DbExtractor\Exception\ColumnNotFoundException;
use Keboola\DbExtractor\ValueObject;

/**
 * @implements IteratorAggregate<Column>
 */
class ColumnCollection implements ValueObject, Countable, IteratorAggregate
{
    /** @var array|Column[] */
    private array $columns;

    /**
     * @param array|Column[] $columns
     */
    public function __construct(array $columns)
    {
        // Check columns type
        array_walk($columns, function ($column): void {
            if (!$column instanceof Column) {
                throw new InvalidArgumentException(sprintf(
                    'All columns must by of type Column, given "%s".',
                    is_object($column) ? get_class($column) : gettype($column)
                ));
            }
        });

        // Sort by ordinalPosition if present
        usort($columns, function (Column $a, Column $b): int {
            $aPos = $a->hasOrdinalPosition() ? $a->getOrdinalPosition() : null;
            $bPos = $b->hasOrdinalPosition() ? $b->getOrdinalPosition() : null;
            return $aPos <=> $bPos;
        });

        $this->columns =  $columns;
    }

    public function count(): int
    {
        return count($this->columns);
    }

    /**
     * @return Iterator<Column>
     */
    public function getIterator(): Iterator
    {
        return new ArrayIterator($this->columns);
    }

    /**
     * @return array|Column[]
     */
    public function getAll(): array
    {
        return $this->columns;
    }

    /**
     * @return array|string[]
     */
    public function getNames(): array
    {
        return array_map(fn(Column $col) => $col->getName(), $this->columns);
    }

    public function isEmpty(): bool
    {
        return empty($this->columns);
    }

    public function getByName(string $name): Column
    {
        foreach ($this->columns as $column) {
            if ($column->getName() === $name) {
                return $column;
            }
        }

        throw new ColumnNotFoundException(sprintf('Column with name "%s" not found.', $name));
    }

    public function getBySanitizedName(string $sanitizedName): Column
    {
        foreach ($this->columns as $column) {
            if ($column->getSanitizedName() === $sanitizedName) {
                return $column;
            }
        }

        throw new ColumnNotFoundException(sprintf('Column with sanitized "%s" not found.'. $sanitizedName));
    }

    public function getByOrdinalPosition(int $ordinalPosition): Column
    {
        foreach ($this->columns as $column) {
            if ($column->getOrdinalPosition() === $ordinalPosition) {
                return $column;
            }
        }

        throw new ColumnNotFoundException(sprintf('Column with ordinal position "%d" not found.', $ordinalPosition));
    }
}
