<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\DbAdapter\QueryResult;

use Iterator;
use IteratorAggregate;

/**
 * @extends IteratorAggregate<array>
 */
interface QueryResult extends IteratorAggregate
{
    /**
     * @return Iterator<array>
     */
    public function getIterator(): Iterator;

    /**
     * @return array<mixed>|null
     */
    public function fetch(): ?array;

    /**
     * @return array<array<mixed>>
     */
    public function fetchAll(): array;

    public function closeCursor(): void;

    /**
     * Returns low-level result resource or object.
     * @return resource|object
     */
    public function getResource();
}
