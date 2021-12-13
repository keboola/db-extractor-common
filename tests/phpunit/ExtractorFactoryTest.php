<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\Common;
use Keboola\DbExtractor\ExtractorFactory;
use Keboola\DbExtractor\Test\ExtractorTest;
use PHPUnit\Framework\Assert;
use Psr\Log\NullLogger;

class ExtractorFactoryTest extends ExtractorTest
{
    public function testCreateExtractor(): void
    {
        $config = $this->getConfig('common');
        $factory = new ExtractorFactory($config['parameters'], []);

        $extractor = $factory->create(
            new NullLogger(),
            'run'
        );

        Assert::assertInstanceOf(Common::class, $extractor);
    }

    public function testUnexistsExtractorClass(): void
    {
        $config = $this->getConfig('common');
        $config['parameters']['extractor_class'] = 'unexists';

        $factory = new ExtractorFactory($config['parameters'], []);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Extractor class \'Keboola\DbExtractor\Extractor\unexists\' doesn\'t exist');
        $factory->create(
            new NullLogger(),
            'run'
        );
    }
}
