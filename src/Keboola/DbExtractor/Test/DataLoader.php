<?php

declare(strict_types=1);

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

    public function load($inputFile, $destinationTable, $ignoreLines = 1)
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

        return $this->pdo->exec($query);
    }

    public function getPdo()
    {
        return $this->pdo;
    }
}
