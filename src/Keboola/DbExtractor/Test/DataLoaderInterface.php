<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Test;

interface DataLoaderInterface
{
    public function createAndUseDb(string $database): void;

    public function createTable(string $tableName, array $columns = [], array $foreignKey = []): void;

    public function dropTable(string $tableName): void;

    public function load(string $inputFile, string $destinationTable, int $ignoreLines = 1): void;

    public function addRows(string $tableName, array $rows): void;
}
