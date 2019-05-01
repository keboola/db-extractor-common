<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\CommonExtractor;

use Keboola\DbExtractor\Test\AbstractExtractorTest;
use Keboola\DbExtractor\Test\DataLoaderInterface;
use PDO;

class CommonExtractorTest extends AbstractExtractorTest
{
    protected function getDataLoader(): DataLoaderInterface
    {
        $host = $this->getEnv(self::DRIVER, 'DB_HOST');
        $port = $this->getEnv(self::DRIVER, 'DB_PORT');
        $dbname = $this->getEnv(self::DRIVER, 'DB_DATABASE');
        $user = $this->getEnv(self::DRIVER, 'DB_USER');
        $pass = $this->getEnv(self::DRIVER, 'DB_PASSWORD');

        $dsn = sprintf("mysql:host=%s;port=%s;dbname=%s;charset=utf8", $host, $port, $dbname);

        $pdo = new PDO(
            $dsn,
            $user,
            $pass,
            [
                PDO::MYSQL_ATTR_LOCAL_INFILE => true,
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            ]
        );
        return new CommonExtractorDataLoader($pdo);
    }

    protected function getDbNameFromEnv(): string
    {
        return getenv('COMMON_DB_DATABASE');
    }

    protected function getDataDir(): string
    {
        return __DIR__ . '/../data';
    }
}
