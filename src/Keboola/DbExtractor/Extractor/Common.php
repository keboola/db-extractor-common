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
}
