<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 12/02/16
 * Time: 16:38
 */

namespace Keboola\DbExtractor\Extractor;

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

    public function listTables()
    {
        $tables = [];
        $res = $this->db->query("SELECT * FROM INFORMATION_SCHEMA.TABLES 
                                  WHERE TABLE_SCHEMA != 'performance_schema' 
                                  AND TABLE_SCHEMA != 'mysql'
                                  AND TABLE_SCHEMA != 'information_schema'");

        $arr = $res->fetchAll(\PDO::FETCH_ASSOC);
        foreach ($arr as $table) {
            $tables[] = [
                'name' => $table['TABLE_NAME'],
                'schema' => $table['TABLE_SCHEMA'],
                'type' => $table['TABLE_TYPE'],
                'numRows' => $table['TABLE_ROWS']
            ];
        }
        return $tables;
    }

    public function describeTable($tableName, $schemaname = null)
    {
        $res = $this->db->query(sprintf("SELECT 
                    c.*, 
                    CONSTRAINT_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME, REFERENCED_TABLE_SCHEMA,
                    FROM INFORMATION_SCHEMA.COLUMNS as c 
                    LEFT OUTER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE as kcu
                    ON c.TABLE_NAME = kcu.TABLE_NAME AND c.COLUMN_NAME = kcu.COLUMN_NAME
                    WHERE c.TABLE_NAME = %s", $this->db->quote($tableName)));
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
                $column[$i]['keyConstraintName'] = $column['CONSTRAINT_NAME'];
            }
            if (!is_null($column['REFERENCED_TABLE_NAME'])) {
                $column[$i]['foreignKeyRefSchema'] = $column['REFERENCED_TABLE_SCHEMA'];
                $column[$i]['foreignKeyRefTable'] = $column['REFERENCED_TABLE_NAME'];
                $column[$i]['foreignKeyRefColumn'] = $column['REFERENCED_COLUMN_NAME'];
            }
        }
        return $columns;
    }
}
