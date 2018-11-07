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
    public function testConfigDatabaseNodeEmptyThrowsException(): void
    {
        $configuration = [
            'parameters' => [
                'db' => [],
            ],
        ];

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('The child node "user" at path "root.parameters.db" must be configured.');
        new BaseExtractorConfig($configuration, new ActionConfigDefinition());
    }

    public function testConfigDatabaseNodeWithUserOnlyThrowsException(): void
    {
        $configuration = [
            'parameters' => [
                'db' => [
                    'user' => 'username',
                ],
            ],
        ];

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('The child node "#password" at path "root.parameters.db" must be configured.');
        new BaseExtractorConfig($configuration, new ActionConfigDefinition());
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

    public function testConfigDatabaseNodeEnabledSshWithEmptyParameters(): void
    {
        $parameters = ConfigParametersProvider::getDbParametersMinimal();
        $parameters['db']['ssh'] = ['enabled' => true];
        $configuration = ['parameters' => $parameters];

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'Invalid configuration for path "root.parameters.db.ssh": Nodes "sshHost" and "keys" are required.'
        );
        new BaseExtractorConfig($configuration, new ActionConfigDefinition());
    }

    public function testConfigDatabaseNodeEnabledSshWithSshHostOnlyThrowsException(): void
    {
        $parameters = ConfigParametersProvider::getDbParametersMinimal();
        $parameters['db']['ssh'] = [
            'enabled' => true,
            'sshHost' => 'some.host',
        ];
        $configuration = ['parameters' => $parameters];

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'Invalid configuration for path "root.parameters.db.ssh": Nodes "sshHost" and "keys" are required.'
        );
        new BaseExtractorConfig($configuration, new ActionConfigDefinition());
    }

    public function testConfigDatabaseNodeEnabledSshWithPrimaryKeyOnlyThrowsException(): void
    {
        $parameters = ConfigParametersProvider::getDbParametersMinimal();
        $parameters['db']['ssh'] = [
            'enabled' => true,
            'keys' => ['#private' => 'private.key'],
        ];
        $configuration = ['parameters' => $parameters];

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage(
            'Invalid configuration for path "root.parameters.db.ssh": Nodes "sshHost" and "keys" are required.'
        );
        new BaseExtractorConfig($configuration, new ActionConfigDefinition());
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
