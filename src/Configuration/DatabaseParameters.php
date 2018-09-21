<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Configuration;

class DatabaseParameters implements DatabaseParametersInterface
{
    /** @var string */
    private $host;

    /** @var int */
    private $port;

    /** @var string|null */
    private $database;

    /** @var string */
    private $user;

    /** @var string */
    private $password;

    /** @var SshParameters|null */
    private $sshParameters;

    public function __construct(
        string $host,
        string $user,
        string $password,
        int $port,
        ?string $database = null,
        ?SshParameters $sshParameters = null
    ) {
        $this->host = $host;
        $this->port = $port;
        $this->database = $database;
        $this->user = $user;
        $this->password = $password;
        $this->sshParameters = $sshParameters;
    }

    public function getHost(): string
    {
        return $this->host;
    }

    public function getPort(): int
    {
        return $this->port;
    }

    public function getDatabase(): ?string
    {
        return $this->database;
    }

    public function getUser(): string
    {
        return $this->user;
    }

    public function getPassword(): string
    {
        return $this->password;
    }

    public function getSsh(): ?SshParameters
    {
        return $this->sshParameters;
    }

    public static function fromRaw(array $databaseParameters): DatabaseParameters
    {
        $host = $databaseParameters['host'];
        $port = (int) $databaseParameters['port'];
        $database = $databaseParameters['database'] ?? null;

        if (!self::isSshEnabled($databaseParameters)) {
            return new DatabaseParameters(
                $host,
                $databaseParameters['user'],
                $databaseParameters['#password'],
                $port,
                $database
            );
        }

        $sshParameters = SshParameters::fromRaw(
            $databaseParameters['ssh'],
            $host,
            $port,
            $databaseParameters['user']
        );
        return new DatabaseParameters(
            '127.0.0.1',
            $databaseParameters['user'],
            $databaseParameters['#password'],
            $sshParameters->getLocalPort(),
            $database,
            $sshParameters
        );
    }

    private static function isSshEnabled(array $databaseParameters): bool
    {
        return isset($databaseParameters['ssh'])
            && isset($databaseParameters['ssh']['enabled'])
            && $databaseParameters['ssh']['enabled'];
    }
}
