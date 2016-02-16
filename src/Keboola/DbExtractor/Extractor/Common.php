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

        $params['password'] = empty($params['password'])?null:$params['password'];
        $dsn = sprintf("mysql:host=%s;dbname=%s;charset=utf8", $params['host'], $params['database']);

        var_dump($params);

        $pdo = new \PDO("mysql:host=localhost;dbname=test;charset=utf8", 'travis', '', [
            \PDO::MYSQL_ATTR_LOCAL_INFILE => true
        ]);

        $pdo->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        $pdo->exec("SET NAMES utf8;");

        return $pdo;
    }
}
