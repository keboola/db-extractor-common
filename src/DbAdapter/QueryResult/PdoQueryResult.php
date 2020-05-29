<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\DbAdapter\QueryResult;

use Iterator;
use PDO;
use PDOStatement;

class PdoQueryResult implements QueryResult
{
    protected PDOStatement $stmt;

    public function __construct(PDOStatement $stmt)
    {
        $this->stmt = $stmt;
    }

    /**
     * @return Iterator<array>
     */
    public function getIterator(): Iterator
    {
        while ($row = $this->stmt->fetch(PDO::FETCH_ASSOC)) {
            yield $row;
        }
    }

    /**
     * @return array<mixed>|null
     */
    public function fetch(): ?array
    {
        $result = $this->stmt->fetch(PDO::FETCH_ASSOC);
        return $result === false ? null : $result;
    }

    /**
     * @return array<array<mixed>>
     */
    public function fetchAll(): array
    {
        /** @var array $result - errrors are converted to exceptions */
        $result = $this->stmt->fetchAll(PDO::FETCH_ASSOC);
        return $result;
    }

    public function closeCursor(): void
    {
        $this->stmt->closeCursor();
    }

    public function getResource(): PDOStatement
    {
        return $this->stmt;
    }
}
