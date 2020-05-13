<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Test;

use PDO;

class DataLoader
{
    private PDO $pdo;

    public function __construct(string $host, string $port, string $dbname, string $user, string $pass)
    {
        $dsn = sprintf('mysql:host=%s;port=%s;dbname=%s;charset=utf8', $host, $port, $dbname);
        $this->pdo = new PDO(
            $dsn,
            $user,
            $pass,
            [
                PDO::MYSQL_ATTR_LOCAL_INFILE => true,
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            ]
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

        /** @var int $result */
        $result = $this->pdo->exec($query);
        return $result;
    }

    public function getPdo(): PDO
    {
        return $this->pdo;
    }
}
