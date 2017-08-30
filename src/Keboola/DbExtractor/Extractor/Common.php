<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 12/02/16
 * Time: 16:38
 */

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;

class Common extends Extractor
{
    public function createConnection($params)
    {
        // convert errors to PDOExceptions
        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
        ];

        // check params
        foreach (['host', 'database', 'user', 'password'] as $r) {
            if (!isset($params[$r])) {
                throw new UserException(sprintf("Parameter %s is missing.", $r));
            }
        }

        $port = isset($params['port']) ? $params['port'] : '3306';
        $dsn = sprintf("mysql:host=%s;port=%s;dbname=%s;charset=utf8", $params['host'], $port, $params['database']);

        $pdo = new \PDO($dsn, $params['user'], $params['password'], $options);
        $pdo->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        $pdo->exec("SET NAMES utf8;");
        return $pdo;
    }

    public function testConnection()
    {
        $this->db->query("SELECT 1");
    }

    public function simpleQuery($table, $columns = array())
    {
        if (count($columns) > 0) {
            return sprintf("SELECT %s FROM %s",
                implode(', ', array_map(function ($column) {
                    return $column;
                }, $columns)),
                $table
            );
        } else {
            return sprintf("SELECT * FROM %s", $this->db->quote($table));
        }
    }

    public function getTables(array $tables = null)
    {
        $sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES 
                                  WHERE TABLE_SCHEMA != 'performance_schema' 
                                  AND TABLE_SCHEMA != 'mysql'
                                  AND TABLE_SCHEMA != 'information_schema'";

        if (!is_null($tables) && count($tables) > 0) {
            $sql .= sprintf(
                " AND TABLE_NAME IN (%s)",
                implode(',', array_map(function ($table) {
                    return $this->db->quote($table);
                }, $tables))
            );
        }

        $res = $this->db->query($sql);
        $arr = $res->fetchAll(\PDO::FETCH_ASSOC);

        $output = [];
        foreach ($arr as $table) {
            $output[] = $this->describeTable($table);
        }
        return $output;
    }

    protected function describeTable(array $table)
    {
        $tabledef = [
            'name' => $table['TABLE_NAME'],
            'schema' => (isset($table['TABLE_SCHEMA'])) ? $table['TABLE_SCHEMA'] : null,
            'type' => (isset($table['TABLE_TYPE'])) ? $table['TABLE_TYPE'] : null,
            'rowCount' => (isset($table['TABLE_ROWS'])) ? $table['TABLE_ROWS'] : null
        ];

        $sql = sprintf("SELECT c.*, 
                    CONSTRAINT_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME, REFERENCED_TABLE_SCHEMA
                    FROM INFORMATION_SCHEMA.COLUMNS as c 
                    LEFT OUTER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE as kcu
                    ON c.TABLE_NAME = kcu.TABLE_NAME AND c.COLUMN_NAME = kcu.COLUMN_NAME
                    WHERE c.TABLE_NAME = %s", $this->db->quote($table['TABLE_NAME']));

        $res = $this->db->query($sql);
        $columns = [];

        $rows = $res->fetchAll(\PDO::FETCH_ASSOC);
        foreach ($rows as $i => $column) {
            $length = ($column['CHARACTER_MAXIMUM_LENGTH']) ? $column['CHARACTER_MAXIMUM_LENGTH'] : null;
            if (is_null($length) && !is_null($column['NUMERIC_PRECISION'])) {
                if ($column['NUMERIC_SCALE'] > 0) {
                    $length = $column['NUMERIC_PRECISION'] . "," . $column['NUMERIC_SCALE'];
                } else {
                    $length = $column['NUMERIC_PRECISION'];
                }
            }
            $columns[] = [
                "name" => $column['COLUMN_NAME'],
                "type" => $column['DATA_TYPE'],
                "primaryKey" => ($column['COLUMN_KEY'] === "PRI") ? true : false,
                "length" => $length,
                "nullable" => ($column['IS_NULLABLE'] === "NO") ? false : true,
                "default" => $column['COLUMN_DEFAULT'],
                "ordinalPosition" => $column['ORDINAL_POSITION']
            ];

            if (!is_null($column['CONSTRAINT_NAME']) ) {
                $columns[$i]['constraintName'] = $column['CONSTRAINT_NAME'];
            }
            if (!is_null($column['REFERENCED_TABLE_NAME'])) {
                $columns[$i]['foreignKeyRefSchema'] = $column['REFERENCED_TABLE_SCHEMA'];
                $columns[$i]['foreignKeyRefTable'] = $column['REFERENCED_TABLE_NAME'];
                $columns[$i]['foreignKeyRefColumn'] = $column['REFERENCED_COLUMN_NAME'];
            }
        }
        $tabledef['columns'] = $columns;
        return $tabledef;
    }
}
