<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\CommonExtractor;

use Keboola\DbExtractor\Test\AbstractExtractorTest;
use Keboola\DbExtractor\Test\AbstractPdoDataLoader;
use UnexpectedValueException;

class CommonExtractorDataLoader extends AbstractPdoDataLoader
{
    public function addRows(string $table, array $rows): void
    {
        if (count($rows) === 0) {
            return;
        }
        $columns = array_keys(reset($rows));

        $rowStrings = $this->getOneRowValuesSqlString($rows);

        $dataString = '(' . implode('),(', $rowStrings) . ')';

        $query = sprintf(
            'INSERT INTO %s (%s) VALUES %s',
            $this->quoteIdentifier($table),
            implode(', ', array_map(function ($column) {
                return $this->quoteIdentifier($column);
            }, $columns)),
            $dataString
        );

        $this->executeQuery($query);
    }

    public function dropTable(string $tableName): void
    {
        $drop = <<<SQL
DROP TABLE IF EXISTS %s
SQL;
        $query = sprintf(
            $drop,
            $this->quoteIdentifier($tableName)
        );
        $this->executeQuery($query);
    }

    protected function generateColumnDefinition(
        string $columnName,
        string $columnType,
        string $columnLength,
        string $columnNullable,
        string $columnDefault
    ): string {
        $result = $this->quoteIdentifier($columnName) . ' ';
        switch ($columnType) {
            case AbstractExtractorTest::COLUMN_TYPE_VARCHAR:
                $result .= 'VARCHAR';
                break;
            case AbstractExtractorTest::COLUMN_TYPE_INTEGER:
                $result .= 'INT';
                break;
            case AbstractExtractorTest::COLUMN_TYPE_AUTOUPDATED_TIMESTAMP:
                $result .= 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP';
                return $result;
            default:
                throw new UnexpectedValueException(sprintf('Unknown column type %s', $columnType));
        }
        if ($columnLength > 0) {
            $result .= '(' . $columnLength . ')';
        }
        if (isset($columnNullable)) {
            $nullable = $columnNullable;
            if ($nullable === true) {
                $result .= ' NULL ';
            } elseif ($nullable === false) {
                $result .= ' NOT NULL ';
            }
        }
        if (isset($columnDefault)) {
            $default = $columnDefault;
            if ($default) {
                $result .= 'DEFAULT ' . $this->quote($default);
            }
        }

        return $result;
    }

    protected function getForeignKeySqlString(
        string $table,
        string $quotedColumnsString,
        string $quotedReferenceColumnsString
    ): string {
        return sprintf(
            'FOREIGN KEY (%s) REFERENCES %s(%s)',
            $quotedColumnsString,
            $this->quoteIdentifier($table),
            $quotedReferenceColumnsString
        );
    }

    /**
     * @param string[] $quotedPkColumnNames
     */
    protected function getPrimaryKeySqlString(array $quotedPkColumnNames): string
    {
        return sprintf('PRIMARY KEY (%s)', implode($this->getColumnInKeyDefinitionSeparator(), $quotedPkColumnNames));
    }

    public function load(string $inputFile, string $destinationTable, int $ignoreLines = 1): void
    {
        $query = sprintf(
            "LOAD DATA LOCAL INFILE '%s'
                INTO TABLE %s
                FIELDS TERMINATED BY ','
                ENCLOSED BY '\"'
                ESCAPED BY ''
                IGNORE %d LINES",
            $inputFile,
            $destinationTable,
            $ignoreLines
        );

        $this->executeQuery($query);
    }

    public function createAndUseDb(string $database): void
    {
        $quotedDatabase = $this->quoteIdentifier($database);
        $this->executeQuery(sprintf(
            'DROP DATABASE IF EXISTS %s',
            $quotedDatabase
        ));
        $this->executeQuery(sprintf(
            '
            CREATE DATABASE %s
            DEFAULT CHARACTER SET utf8
            DEFAULT COLLATE utf8_general_ci
            ',
            $quotedDatabase
        ));
        $this->executeQuery('USE ' . $quotedDatabase);
        $this->executeQuery('SET NAMES utf8;');
    }

    public function createTable(string $name, array $columns = [], array $foreignKey = []): void
    {
        $this->dropTable($name);

        $columnsDefinition = $this->getColumnsDefinition($columns);
        $primaryKeyDefinition = $this->getPrimaryKeyDefintion($columns);
        $foreignKeyDefintion = $this->getForeignKeyDefintion($foreignKey);

        $sql = <<<QUERY
CREATE TABLE %s (
  %s
  %s
  %s
)
QUERY;

        $query = sprintf(
            $sql,
            $this->quoteIdentifier($name),
            $columnsDefinition,
            $primaryKeyDefinition,
            $foreignKeyDefintion
        );

        $this->executeQuery($query);
    }
}
