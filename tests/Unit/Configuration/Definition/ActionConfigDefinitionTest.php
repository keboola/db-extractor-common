<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Tests\Unit\Configuration\Definition;

use Keboola\DbExtractorCommon\Configuration\BaseExtractorConfig;
use Keboola\DbExtractorCommon\Configuration\Definition\ActionConfigDefinition;
use Keboola\DbExtractorCommon\Tests\Unit\Configuration\__fixtures\ConfigParametersProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ActionConfigDefinitionTest extends TestCase
{

    /**
     * @dataProvider invalidConfigurationData
     */
    public function testInvalidConfigurationThrowsException(array $parameters, string $exceptionMessage): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage($exceptionMessage);
        new BaseExtractorConfig(['parameters' => $parameters], new ActionConfigDefinition());
    }

    /**
     * @dataProvider validConfigurationData
     */
    public function testValidConfiguration(array $parameters): void
    {
        $config = new BaseExtractorConfig(['parameters' => $parameters], new ActionConfigDefinition());
        $this->assertInstanceOf(BaseExtractorConfig::class, $config);
    }

    public function invalidConfigurationData(): array
    {
        return [
            [
                ['db' => []],
                'The child node "user" at path "root.parameters.db" must be configured.',
            ],
            [
                ['db' => ['user' => 'username']],
                'The child node "#password" at path "root.parameters.db" must be configured.',
            ],
            [
                ConfigParametersProvider::getDbParametersMinimalEnabledSshWithEmptyParameters(),
                'Invalid configuration for path "root.parameters.db.ssh": Nodes "sshHost" and "keys" are required.',
            ],
            [
                ConfigParametersProvider::getDbParametersMinimalEnabledSshWithPSshHostOnly(),
                'Invalid configuration for path "root.parameters.db.ssh": Nodes "sshHost" and "keys" are required.',
            ],
            [
                ConfigParametersProvider::getDbParametersMinimalEnabledSshWithPSshHostOnly(),
                'Invalid configuration for path "root.parameters.db.ssh": Nodes "sshHost" and "keys" are required.',
            ],
        ];
    }

    public function validConfigurationData(): array
    {
        return [
            [ConfigParametersProvider::getDbParametersMinimal()],
            [ConfigParametersProvider::getDbParametersBasic()],
            [ConfigParametersProvider::getDbParametersMinimalWithSshEmptyParameters()],
            [ConfigParametersProvider::getDbParametersMinimalDisabledSshWithEmptyParameters()],
            [ConfigParametersProvider::getDbParametersMinimalWithSshMinimal()],
            [ConfigParametersProvider::getDbParametersMinimalWithSshBasic()],
        ];
    }
}
