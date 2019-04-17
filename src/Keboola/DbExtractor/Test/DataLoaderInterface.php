<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Test;

interface DataLoaderInterface
{
    public function createAndUseDb(string $database): void;

    public function createAutoIncrementTable(): void;

    public function createTable(string $name, array $columns = [], array $foreignKey = []): void;

    public function load(string $inputFile, string $destinationTable, int $ignoreLines = 1): int;

    public function addRows(string $table, array $rows): void;
}
