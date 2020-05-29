<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\DbAdapter\QueryResult;

use Iterator;

class OdbcQueryResult implements QueryResult
{
    /** @var resource */
    protected $stmt;

    protected bool $closed = false;

    /**
     * @param resource $stmt
     */
    public function __construct($stmt)
    {
        $this->stmt = $stmt;
    }

    public function __destruct()
    {
        $this->closeCursor();
    }

    /**
     * @return Iterator<array>
     */
    public function getIterator(): Iterator
    {
        while ($row = odbc_fetch_array($this->stmt)) {
            yield $row;
        }
    }

    /**
     * @return array<mixed>|null
     */
    public function fetch(): ?array
    {
        /** @var array|false $result */
        $result = odbc_fetch_array($this->stmt);
        return $result === false ? null : $result;
    }

    /**
     * @return array<array<mixed>>
     */
    public function fetchAll(): array
    {
        return iterator_to_array($this->getIterator());
    }

    public function closeCursor(): void
    {
        if ($this->closed === false) {
            odbc_free_result($this->stmt);
            $this->closed = true;
        }
    }

    /**
     * @return resource
     */
    public function getResource()
    {
        return $this->stmt;
    }
}
