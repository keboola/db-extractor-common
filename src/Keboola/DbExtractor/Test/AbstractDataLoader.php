<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Test;

use const PHP_EOL;

abstract class AbstractDataLoader implements DataLoaderInterface
{
    abstract public function addRows(string $tableName, array $rows): void;

    abstract public function createAndUseDb(string $database): void;

    abstract public function createTable(string $tableName, array $columns = [], array $foreignKey = []): void;

    abstract public function dropTable(string $tableName): void;

    abstract public function load(string $inputFile, string $destinationTable, int $ignoreLines = 1): void;

    abstract protected function executeQuery(string $query): void;

    abstract protected function generateColumnDefinition(
        string $columnName,
        string $columnType,
        string $columnLength,
        string $columnNullable,
        string $columnDefault
    ): string;

    abstract protected function getForeignKeySqlString(
        string $table,
        string $quotedColumnsString,
        string $quotedReferenceColumnsString
    ): string;

    abstract protected function getPrimaryKeySqlString(array $quotedPkColumnNames): string;

    abstract protected function quote(string $string): string;

    abstract protected function quoteIdentifier(string $identifier): string;

    protected function getColumnsDefinition(array $columns): string
    {
        $columns = array_map(function (array $column): string {
            return $this->generateColumnDefinition(
                $column['name'],
                $column['type'],
                $column['length'],
                $column['nullable'],
                $column['default']
            );
        }, $columns);
        return implode($this->getColumnDefintionSeparator() . PHP_EOL, $columns);
    }

    protected function getColumnDefintionSeparator(): string
    {
        return $this->getColumnSeparator();
    }

    protected function getColumnSeparator(): string
    {
        return ',';
    }

    protected function getForeignKeyDefintion(array $foreignKey): string
    {
        if (!count($foreignKey)) {
            return '';
        }
        $quotedColumnsString = implode($this->getColumnInKeyDefinitionSeparator(), array_map(function ($column
        ): string {
            return $this->quoteIdentifier($column);
        }, $foreignKey['columns']));
        $quotedReferenceColumnsString = implode($this->getColumnInKeyDefinitionSeparator(), array_map(function ($column
        ): string {
            return $this->quoteIdentifier($column);
        }, $foreignKey['references']['columns']));
        return ',' .
            $this->getForeignKeySqlString(
                $foreignKey['references']['table'],
                $quotedColumnsString,
                $quotedReferenceColumnsString
            );
    }

    protected function getColumnInKeyDefinitionSeparator(): string
    {
        return $this->getColumnSeparator();
    }

    /**
     * @param array $rows
     * @return array
     */
    protected function getOneRowValuesSqlString(array $rows): array
    {
        $rowStrings = array_map(function ($row) {
            $quotedColumns = array_map(function ($column) {
                return $this->quote($column);
            }, $row);
            return implode($this->getColumnSeparator(), $quotedColumns);
        }, $rows);
        return $rowStrings;
    }

    protected function getPrimaryKeyDefintion(array $columns): string
    {
        $quotedPkColumnNames = $this->getQuotedPkColumnNames($columns);
        if (count($quotedPkColumnNames)) {
            return $this->getColumnDefintionSeparator() . $this->getPrimaryKeySqlString($quotedPkColumnNames);
        }
        return '';
    }

    /**
     * @param array[] $columns
     * @return string[]
     */
    protected function getQuotedPkColumnNames(array $columns): array
    {
        $pkColumns = array_filter($columns, function ($column) {
            return $column['primaryKey'] === true;
        });
        $quotedPkColumnNames = array_map(function ($column) {
            return $this->quoteIdentifier($column['name']);
        }, $pkColumns);
        return $quotedPkColumnNames;
    }
}
