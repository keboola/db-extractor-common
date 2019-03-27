<?php

declare(strict_types=1);



namespace Keboola\DbExtractor\Tests;
require_once __DIR__ . '/../src/Keboola/DbExtractor/Test/AbstractExtractorTest.php';

use Keboola\DbExtractor\Test\CommonDataLoader;

class CommonExtractorTest extends AbstractExtractorTest
{
    protected function getDataLoader()
    {
        return new CommonDataLoader(
            $this->getEnv(self::DRIVER, 'DB_HOST'),
            $this->getEnv(self::DRIVER, 'DB_PORT'),
            $this->getEnv(self::DRIVER, 'DB_DATABASE'),
            $this->getEnv(self::DRIVER, 'DB_USER'),
            $this->getEnv(self::DRIVER, 'DB_PASSWORD')
        );
    }
}
