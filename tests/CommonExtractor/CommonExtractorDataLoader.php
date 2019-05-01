<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\CommonExtractor;

use Keboola\DbExtractor\Test\AbstractExtractorTest;
use Keboola\DbExtractor\Test\AbstractPdoDataLoader;
use UnexpectedValueException;

class CommonExtractorDataLoader extends AbstractPdoDataLoader
{
    protected function generateColumnDefinition(
        string $columnName,
        string $columnType,
        ?string $columnLength,
        ?bool $columnNullable,
        ?string $columnDefault,
        ?bool $isPrimary
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
        if ($columnNullable !== null) {
            $nullable = $columnNullable;
            if ($nullable === true) {
                $result .= ' NULL ';
            } elseif ($nullable === false) {
                $result .= ' NOT NULL ';
            }
        }
        if ($isPrimary && $columnType === AbstractExtractorTest::COLUMN_TYPE_INTEGER) {
            $result .= ' AUTO_INCREMENT';
        }
        if ($columnDefault !== null) {
            $default = $columnDefault;
            if ($default) {
                $result .= 'DEFAULT ' . $this->quote($default);
            }
        }

        return $result;
    }

    protected function getForeignKeySqlString(
        string $quotedTableName,
        string $quotedColumnsString,
        string $quotedReferenceColumnsString
    ): string {
        return sprintf(
            'FOREIGN KEY (%s) REFERENCES %s(%s)',
            $quotedColumnsString,
            $quotedTableName,
            $quotedReferenceColumnsString
        );
    }

    protected function getPrimaryKeySqlString(string $primaryKeyColumnsString): string
    {
        return sprintf('PRIMARY KEY (%s)', $primaryKeyColumnsString);
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

    protected function getCreateTableQuery(
        string $quotedTableName,
        string $columnsDefinition,
        string $primaryKeyDefinition,
        string $foreignKeyDefintion
    ): string {
        $sql = <<<QUERY
CREATE TABLE %s (
  %s
  %s
  %s
)
QUERY;

        $query = sprintf(
            $sql,
            $quotedTableName,
            $columnsDefinition,
            $primaryKeyDefinition,
            $foreignKeyDefintion
        );
        return $query;
    }

    protected function getInsertSqlQuery(
        string $quotedTableName,
        ?string $quotedTableColumnsSqlString,
        string $valuesString
    ): string {
        $query = sprintf(
            'INSERT INTO %s %s VALUES %s',
            $quotedTableName,
            $quotedTableColumnsSqlString === null ? '' : '(' . $quotedTableColumnsSqlString . ')',
            $valuesString
        );
        return $query;
    }

    protected function getDropTableSqlQuery(string $quotedTableName): string
    {
        $query = sprintf(
            'DROP TABLE IF EXISTS %s',
            $quotedTableName
        );
        return $query;
    }
}
