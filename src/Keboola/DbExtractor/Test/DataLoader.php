<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 19/02/16
 * Time: 11:06
 */

namespace Keboola\DbExtractor\Test;

class DataLoader
{
    private $pdo;

    public function __construct($host, $port, $dbname, $user, $pass)
    {
        $dsn = sprintf("mysql:host=%s;port=%s;dbname=%s;charset=utf8", $host, $port, $dbname);
        $this->pdo = new \PDO($dsn, $user, $pass, [
            \PDO::MYSQL_ATTR_LOCAL_INFILE => true
        ]);
    }

    public function load($inputFile, $destinationTable)
    {
        $query = sprintf(
            "LOAD DATA LOCAL INFILE '%s'
                INTO TABLE %s
                FIELDS TERMINATED BY ','
                ENCLOSED BY '\"'
                ESCAPED BY ''
                IGNORE 1 LINES",
            $inputFile,
            $destinationTable
        );

        return $this->pdo->exec($query);
    }

    public function getPdo()
    {
        return $this->pdo;
    }
}
