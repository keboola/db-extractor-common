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
        $res = $this->db->query("SHOW TABLES");
        while ($table = $res->fetch(\PDO::FETCH_NUM)) {
            $tables[] = $table[0];
        }
        return $tables;
    }

    public function describeTable($tableName)
    {
        $res = $this->db->query(sprintf("SELECT 
                    COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, 
                    CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_KEY
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = %s", $this->db->quote($tableName)));
        $columns = [];
        while ($column = $res->fetch(\PDO::FETCH_ASSOC)) {
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
                "default" => $column['COLUMN_DEFAULT']
            ];
        }
        return $columns;
    }
}
