<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Configuration;

class DatabaseParameters implements DatabaseParametersInterface
{
    /** @var int */
    protected $defaultDatabasePort = 3306;

    /** @var string */
    private $host;

    /** @var int|null */
    private $port;

    /** @var string|null */
    private $database;

    /** @var string */
    private $user;

    /** @var string */
    private $password;

    /** @var SshParametersInterface */
    private $sshParameters;

    public function __construct(
        string $host,
        string $user,
        string $password,
        ?string $database = null,
        ?int $port = null,
        ?SshParametersInterface $sshParameters = null
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
        if ($this->port) {
            return $this->port;
        }
        return $this->defaultDatabasePort;
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

    public function getSsh(): ?SshParametersInterface
    {
        return $this->sshParameters;
    }

    public static function fromRaw(array $databaseParameters): DatabaseParameters
    {
        $host = $databaseParameters['host'];
        $port = isset($databaseParameters['port']) ? (int) $databaseParameters['port'] : null;
        $database = $databaseParameters['database'] ?? null;

        if (!self::isSshEnabled($databaseParameters)) {
            return new DatabaseParameters(
                $host,
                $databaseParameters['user'],
                $databaseParameters['#password'],
                $database,
                $port
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
            $database,
            $sshParameters->getLocalPort(),
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
