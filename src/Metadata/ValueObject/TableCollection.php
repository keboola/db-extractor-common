<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Metadata\ValueObject;

use Countable;
use Iterator;
use IteratorAggregate;
use ArrayIterator;
use Keboola\DbExtractor\Exception\TableNotFoundException;
use Keboola\DbExtractor\ValueObject;

/**
 * @implements IteratorAggregate<Table>
 */
class TableCollection implements ValueObject, Countable, IteratorAggregate
{
    /** @var array|Table[] */
    private array $tables;

    /**
     * @param array|Table[] $tables
     */
    public function __construct(array $tables)
    {
        $this->tables =  $tables;
    }

    public function count(): int
    {
        return count($this->tables);
    }

    /**
     * @return Iterator<Table>
     */
    public function getIterator(): Iterator
    {
        return new ArrayIterator($this->tables);
    }

    /**
     * @return array|Table[]
     */
    public function getAll(): array
    {
        return $this->tables;
    }

    public function isEmpty(): bool
    {
        return empty($this->tables);
    }

    public function getByNameAndSchema(string $name, string $schema): Table
    {
        foreach ($this->tables as $table) {
            if ($table->getName() === $name && $table->getSchema() === $schema) {
                return $table;
            }
        }

        throw new TableNotFoundException(sprintf(
            'Table with name "%s" and schema "%s" not found.',
            $name,
            $schema
        ));
    }
}
