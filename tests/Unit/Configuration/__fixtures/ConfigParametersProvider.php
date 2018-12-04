<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Tests\Unit\Configuration\__fixtures;

class ConfigParametersProvider
{
    public static function getDbParametersBasic(): array
    {
        return ['db' => self::getDbNodeBasic()];
    }

    public static function getDbParametersMinimal(): array
    {
        return ['db' => self::getDbNodeMinimal()];
    }

    public static function getDbParametersMinimalWithSshEmptyParameters(): array
    {
        $dbNode = self::getDbNodeMinimal();
        $dbNode['ssh'] = [];
        return ['db' => $dbNode];
    }

    public static function getDbParametersMinimalDisabledSshWithEmptyParameters(): array
    {
        $dbNode = self::getDbNodeMinimal();
        $dbNode['ssh'] = ['enabled' => false];
        return ['db' => $dbNode];
    }

    public static function getDbParametersMinimalEnabledSshWithEmptyParameters(): array
    {
        $dbNode = self::getDbNodeMinimal();
        $dbNode['ssh'] = ['enabled' => true];
        return ['db' => $dbNode];
    }

    public static function getDbParametersMinimalEnabledSshWithPrivateKeyOnly(): array
    {
        $dbNode = self::getDbNodeMinimal();
        $dbNode['ssh'] = [
            'enabled' => true,
            'keys' => ['#private' => 'priv.key'],
        ];
        return ['db' => $dbNode];
    }

    public static function getDbParametersMinimalEnabledSshWithPSshHostOnly(): array
    {
        $dbNode = self::getDbNodeMinimal();
        $dbNode['ssh'] = [
            'enabled' => true,
            'sshHost' => 'some.host',
        ];
        return ['db' => $dbNode];
    }

    public static function getDbParametersMinimalWithSshMinimal(): array
    {
        $dbNode = self::getDbNodeMinimal();
        $dbNode['ssh'] = self::getSshNodeMinimal();

        return ['db' => $dbNode];
    }

    public static function getDbParametersMinimalWithSshBasic(): array
    {
        $dbNode = self::getDbNodeMinimal();
        $dbNode['ssh'] = self::getSshNodeBasic();
        return ['db' => $dbNode];
    }

    private static function getDbNodeBasic(): array
    {
        return [
            'host' => 'hostname',
            'port' => '10002',
            'user' => 'username',
            '#password' => 'pw',
            'database' => 'database_name',
        ];
    }

    private static function getDbNodeMinimal(): array
    {
        return [
            'user' => 'username',
            '#password' => 'pw',
        ];
    }

    private static function getSshNodeBasic(): array
    {
        return [
            'enabled' => true,
            'keys' => [
                'private' => 'private',
                '#private' => 'private',
                'public' => 'public',
            ],
            'sshHost' => 'hostname',
            'sshPort' => 22,
            'remoteHost' => 'remote.hostname',
            'remotePort' => 33036,
            'user' => 'username',
        ];
    }

    private static function getSshNodeMinimal(): array
    {
        return [
            'enabled' => true,
            'sshHost' => 'hostname',
            'keys' => [
                '#private' => 'private.key===',
            ],
        ];
    }
}
