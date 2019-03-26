<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Test;

abstract class AbstractDataLoader
{
    abstract public function createAndUseDb(string $database): void;

    abstract public function createTable(string $name, array $columns = [], array $foreignKey = []): void;

    abstract public function load(string $inputFile, string $destinationTable, int $ignoreLines = 1): int;

    abstract public function addRow(string $table, array $data): ?int;
}
