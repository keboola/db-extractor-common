<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Test;

use const PHP_EOL;

abstract class AbstractDataLoader implements DataLoaderInterface
{
    abstract public function createAndUseDb(string $database): void;

    abstract public function load(string $inputFile, string $destinationTable, int $ignoreLines = 1): void;

    abstract protected function executeQuery(string $query): void;

    abstract protected function generateColumnDefinition(
        string $columnName,
        string $columnType,
        ?string $columnLength,
        ?bool $columnNullable,
        ?string $columnDefault,
        ?bool $isPrimary
    ): string;

    abstract protected function getForeignKeySqlString(
        string $quotedTableName,
        string $quotedColumnsString,
        string $quotedReferenceColumnsString
    ): string;

    abstract protected function getPrimaryKeySqlString(string $primaryKeyColumnsString): string;

    abstract protected function quote(string $string): string;

    abstract protected function quoteIdentifier(string $identifier): string;

    abstract protected function getCreateTableQuery(
        string $quotedTableName,
        string $columnsDefinition,
        string $primaryKeyDefinition,
        string $foreignKeyDefintion
    ): string;

    abstract protected function getInsertSqlQuery(
        string $quotedTableName,
        string $quotedTableColumnsSqlString,
        string $valuesString
    ): string;

    abstract protected function getDropTableSqlQuery(string $quotedTableName): string;

    protected function getColumnsDefinition(array $columns): string
    {
        $columns = array_map(function (array $column): string {
            return $this->generateColumnDefinition(
                $column['name'],
                $column['type'],
                isset($column['length']) ? $column['length'] : null,
                isset($column['nullable']) ? $column['nullable'] : null,
                isset($column['default']) ? (string) $column['default'] : null,
                isset($column['primaryKey']) ? $column['primaryKey'] : null
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
                $this->quoteIdentifier($foreignKey['references']['table']),
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
    protected function mapRowsToRowValuesSqlStrings(array $rows): array
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
            $pkColumnsString = implode($this->getColumnInKeyDefinitionSeparator(), $quotedPkColumnNames);
            return $this->getColumnDefintionSeparator() . $this->getPrimaryKeySqlString($pkColumnsString);
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
            if (!isset($column['primaryKey'])) {
                return false;
            }
            return $column['primaryKey'] === true;
        });
        $quotedPkColumnNames = array_map(function ($column) {
            return $this->quoteIdentifier($column['name']);
        }, $pkColumns);
        return $quotedPkColumnNames;
    }

    public function createTable(string $name, array $columns = [], array $foreignKey = []): void
    {
        $this->dropTable($name);

        $columnsDefinition = $this->getColumnsDefinition($columns);
        $primaryKeyDefinition = $this->getPrimaryKeyDefintion($columns);
        $foreignKeyDefintion = $this->getForeignKeyDefintion($foreignKey);

        $query = $this->getCreateTableQuery($this->quoteIdentifier($name), $columnsDefinition, $primaryKeyDefinition, $foreignKeyDefintion);

        $this->executeQuery($query);
    }

    public function addRows(string $table, array $rows): void
    {
        if (count($rows) === 0) {
            return;
        }
        $columns = array_keys(reset($rows));

        $quotedTableColumnsSqlString = implode(', ', array_map(function ($column) {
            return $this->quoteIdentifier($column);
        }, $columns));
        $quotedTableName = $this->quoteIdentifier($table);
        $valuesString = $this->mapRowValueStringsToAllRowsValueString($this->mapRowsToRowValuesSqlStrings($rows));
        $query = $this->getInsertSqlQuery($quotedTableName, $quotedTableColumnsSqlString, $valuesString);

        $this->executeQuery($query);
    }

    public function dropTable(string $tableName): void
    {
        $quotedTableName = $this->quoteIdentifier($tableName);
        $query = $this->getDropTableSqlQuery($quotedTableName);
        $this->executeQuery($query);
    }

    /**
     * @param array $rowValuesSqlStrings
     * @return string
     */
    protected function mapRowValueStringsToAllRowsValueString(array $rowValuesSqlStrings): string
    {
        return '(' . implode('),(', $rowValuesSqlStrings) . ')';
    }
}
