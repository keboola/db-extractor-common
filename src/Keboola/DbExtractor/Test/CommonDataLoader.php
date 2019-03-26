<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Test;

use Exception;
use Keboola\DbExtractor\Tests\AbstractExtractorTest;
use PDO;

class CommonDataLoader extends AbstractDataLoader
{
    /** @var PDO */
    private $db;

    public function __construct(string $host, string $port, string $dbname, string $user, string $pass)
    {
        $dsn = sprintf("mysql:host=%s;port=%s;dbname=%s;charset=utf8", $host, $port, $dbname);
        $this->db = new PDO(
            $dsn,
            $user,
            $pass,
            [
                PDO::MYSQL_ATTR_LOCAL_INFILE => true,
            ]
        );
    }

    private function getPrimaryKeyDefintion(array $columns): string
    {
        $pkColumns = array_filter($columns, function ($column) {
            return $column['primaryKey'] === true;
        });
        $quotedPkColumnNames = array_map(function ($column) {
            return $this->quoteIdentifier($column['name']);
        }, $pkColumns);
        if (count($quotedPkColumnNames)) {
            return ',PRIMARY KEY (' . implode(',', $quotedPkColumnNames) . ')';
        }
        return '';
    }

    private function getColumnsDefinition(array $columns): string
    {
        $columns = array_map(function (array $column) {
            $result = $column['name'];
            switch ($column['type']) {
                case AbstractExtractorTest::VARCHAR:
                    $result .= 'VARCHAR';
                    break;
                default:
                    throw new Exception(sprintf('Unknown column type %s', $column['type']));
            }
            if ($column['length'] > 0) {
                $result .= '(' . $column['length'] . ')';
            }
            if ($column['nullable'] === true) {
                $result .= ' NULL ';
            } elseif ($column['nullable'] === false) {
                $result .= ' NOT NULL ';
            }
            if ($column['default']) {
                $result .= 'DEFAULT ' . $this->quote($column['default']);
            }
            return $result;
        }, $columns);
        return implode(',', $columns);
    }

    private function getForeignKeyDefintion(array $foreignKey): string
    {
        if (!count($foreignKey)) {
            return '';
        }
        $quotedColumnsString = implode(',', array_map(function ($column): string {
            return $this->quoteIdentifier($column);
        }, $foreignKey['columns']));
        $quotedReferenceColumnsString = implode(',', array_map(function ($column): string {
            return $this->quoteIdentifier($column);
        }, $foreignKey['references']['columns']));
        return sprintf(
            'FOREIGN KEY (%s) REFERENCES %s(%s))',
            $quotedColumnsString,
            $this->quoteIdentifier($foreignKey['references']['table']),
            $quotedReferenceColumnsString
        );
    }

    public function addRow(string $table, array $data): ?int
    {
        $columns = array_keys($data);
        $dataString = implode('),(', $rows);
        return sprintf(
            'INSERT INTO %s (%s) VALUES %s',
            $this->quoteIdentifier($table),
            implode(', ', array_map(function ($column) {
                return $this->quoteIdentifier($column);
            }, $columns)),

        );
    }

    public function load(string $inputFile, string $destinationTable, int $ignoreLines = 1): int
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

        return $this->db->exec($query);
    }

    public function createAndUseDb(string $database): void
    {
        $this->db->exec(sprintf(
            'DROP DATABASE IF EXISTS `%s`',
            $this->quoteIdentifier($database)
        ));
        $this->db->exec(sprintf(
            '
            CREATE DATABASE `%s`
            DEFAULT CHARACTER SET utf8
            DEFAULT COLLATE utf8_general_ci
            ',
            $this->quoteIdentifier($database)
        ));
        $this->db->exec('USE ' . $database);
        $this->db->exec('SET NAMES utf8;');
    }

    public function createTable(string $name, array $columns, array $foreignKey): void
    {
        $columnsDefinition = $this->getColumnsDefinition($columns);
        $primaryKeyDefinition = $this->getPrimaryKeyDefintion($columns);
        $foreignKeyDefintion = $this->getForeignKeyDefintion($foreignKey);
        $sql = <<<SQL
CREATE TABLE %s (
  %s
  %s
  %s
)
SQL;

        $this->db->exec(sprintf(
            $sql,
            $this->quoteIdentifier($name),
            $columnsDefinition,
            $primaryKeyDefinition,
            $foreignKeyDefintion
        ));
    }

    public function quote(string $string): string
    {
        return $this->db->quote($string);
    }

    public function quoteIdentifier(string $identifier): string
    {
        return '`' . str_replace('`', '``', $identifier) . '`';
    }
}
