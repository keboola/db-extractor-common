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

    public function testConfigDatabaseNodeWithMinimumParameters(): void
    {
        $configuration = [
            'parameters' => ConfigParametersProvider::getDbParametersMinimal(),
        ];
        $config = new BaseExtractorConfig($configuration, new ActionConfigDefinition());
        $this->assertInstanceOf(BaseExtractorConfig::class, $config);
    }

    public function testConfigDatabaseNodeWithBasicParameters(): void
    {
        $configuration = [
            'parameters' => ConfigParametersProvider::getDbParametersBasic(),
        ];
        $config = new BaseExtractorConfig($configuration, new ActionConfigDefinition());
        $this->assertInstanceOf(BaseExtractorConfig::class, $config);
    }

    public function testConfigDatabaseNodeWithSshEmpty(): void
    {
        $parameters = ConfigParametersProvider::getDbParametersMinimal();
        $parameters['db']['ssh'] = [];
        $configuration = ['parameters' => $parameters];

        $config = new BaseExtractorConfig($configuration, new ActionConfigDefinition());
        $this->assertInstanceOf(BaseExtractorConfig::class, $config);
    }

    public function testConfigDatabaseNodeDisabledSshWithEmptyParameters(): void
    {
        $parameters = ConfigParametersProvider::getDbParametersMinimal();
        $parameters['db']['ssh'] = ['enabled' => false];
        $configuration = ['parameters' => $parameters];

        $config = new BaseExtractorConfig($configuration, new ActionConfigDefinition());
        $this->assertInstanceOf(BaseExtractorConfig::class, $config);
    }

    public function testConfigDatabaseNodeEnabledSshWithMinimumParameters(): void
    {
        $configuration = ['parameters' => ConfigParametersProvider::getDbParametersMinimalWithSshMinimal()];

        $config = new BaseExtractorConfig($configuration, new ActionConfigDefinition());
        $this->assertInstanceOf(BaseExtractorConfig::class, $config);
    }

    public function testConfigDatabaseNodeEnabledSshWithBasicParameters(): void
    {
        $configuration = ['parameters' => ConfigParametersProvider::getDbParametersMinimalWithSshBasic()];

        $config = new BaseExtractorConfig($configuration, new ActionConfigDefinition());
        $this->assertInstanceOf(BaseExtractorConfig::class, $config);
    }
}
