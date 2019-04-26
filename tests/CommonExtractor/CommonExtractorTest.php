<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests\CommonExtractor;

use Keboola\DbExtractor\Test\AbstractExtractorTest;
use Keboola\DbExtractor\Test\DataLoaderInterface;

class CommonExtractorTest extends AbstractExtractorTest
{
    protected function getDataLoader(): DataLoaderInterface
    {
        return new CommonExtractorDataLoader(
            $this->getEnv(self::DRIVER, 'DB_HOST'),
            $this->getEnv(self::DRIVER, 'DB_PORT'),
            $this->getEnv(self::DRIVER, 'DB_DATABASE'),
            $this->getEnv(self::DRIVER, 'DB_USER'),
            $this->getEnv(self::DRIVER, 'DB_PASSWORD')
        );
    }
}
