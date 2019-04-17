<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Test;

use Exception;
use PDO;

class CommonExtractorDataLoader implements DataLoaderInterface
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
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
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
            $result = $this->quoteIdentifier($column['name']) . ' ';
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
            if (isset($column['nullable'])) {
                $nullable = $column['nullable'];
                if ($nullable === true) {
                    $result .= ' NULL ';
                } elseif ($nullable === false) {
                    $result .= ' NOT NULL ';
                }
            }
            if (isset($column['default'])) {
                $default = $column['default'];
                if ($default) {
                    $result .= 'DEFAULT ' . $this->quote($default);
                }
            }

            return $result;
        }, $columns);
        return implode(',' . \PHP_EOL, $columns);
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
            ',FOREIGN KEY (%s) REFERENCES %s(%s)',
            $quotedColumnsString,
            $this->quoteIdentifier($foreignKey['references']['table']),
            $quotedReferenceColumnsString
        );
    }

    public function addRows(string $table, array $rows): void
    {
        if (count($rows) === 0) {
            return;
        }
        $columns = array_keys(reset($rows));
        $rowStrings = array_map(function ($row) {
            $quotedColumns = array_map(function ($column) {
                return $this->quote($column);
            }, $row);
            return implode(', ', $quotedColumns);
        }, $rows);
        $dataString = '(' . implode('),(', $rowStrings) . ')';
        $query = sprintf(
            'INSERT INTO %s (%s) VALUES %s',
            $this->quoteIdentifier($table),
            implode(', ', array_map(function ($column) {
                return $this->quoteIdentifier($column);
            }, $columns)),
            $dataString
        );

        $this->db->exec($query);
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
        $quotedDatabase = $this->quoteIdentifier($database);
        $this->db->exec(sprintf(
            'DROP DATABASE IF EXISTS %s',
            $quotedDatabase
        ));
        $this->db->exec(sprintf(
            '
            CREATE DATABASE %s
            DEFAULT CHARACTER SET utf8
            DEFAULT COLLATE utf8_general_ci
            ',
            $quotedDatabase
        ));
        $this->db->exec('USE ' . $quotedDatabase);
        $this->db->exec('SET NAMES utf8;');
    }

    public function createTable(string $name, array $columns = [], array $foreignKey = []): void
    {
        $columnsDefinition = $this->getColumnsDefinition($columns);
        $primaryKeyDefinition = $this->getPrimaryKeyDefintion($columns);
        $foreignKeyDefintion = $this->getForeignKeyDefintion($foreignKey);
        $drop = <<<SQL
DROP TABLE IF EXISTS %s
SQL;
        $query = sprintf(
            $drop,
            $this->quoteIdentifier($name)
        );
        $this->db->exec($query);

        $sql = <<<SQL
CREATE TABLE %s (
  %s
  %s
  %s
)
SQL;

        $query = sprintf(
            $sql,
            $this->quoteIdentifier($name),
            $columnsDefinition,
            $primaryKeyDefinition,
            $foreignKeyDefintion
        );
        $this->db->exec($query);
    }

    public function quote(string $string): string
    {
        return $this->db->quote($string);
    }

    public function quoteIdentifier(string $identifier): string
    {
        return '`' . str_replace('`', '``', $identifier) . '`';
    }

    public function createAutoIncrementTable(): void
    {
        $this->db->exec('DROP TABLE IF EXISTS auto_increment_timestamp');

        $this->db->exec(
            'CREATE TABLE auto_increment_timestamp (
            `id` INT NOT NULL AUTO_INCREMENT,
            `name` VARCHAR(30) NOT NULL DEFAULT \'pam\',
            `timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`)
        )'
        );
    }
}
