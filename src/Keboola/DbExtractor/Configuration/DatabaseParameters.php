<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

class DatabaseParameters
{
    /** @var string */
    private $host;

    /** @var int|null */
    private $port;

    /** @var string */
    private $database;

    /** @var string */
    private $user;

    /** @var string */
    private $password;

    /** @var SshParameters|null */
    private $sshParameters;

    public function __construct(array $databaseParameters)
    {
        $this->host = $databaseParameters['host'];
        $this->port = $databaseParameters['port'] ?? null;
        $this->database = $databaseParameters['database'];
        $this->user = $databaseParameters['user'];
        $this->password = $databaseParameters['#password'];
        $this->sshParameters = isset($databaseParameters['ssh'])
            ? SshParameters::fromRaw($databaseParameters['ssh'])
            : null;
    }

    public function getHost(): string
    {
        return $this->host;
    }

    public function getPort(): ?int
    {
        return (int) $this->port;
    }

    public function getDatabase(): string
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

    public function getSshParameters(): ?SshParameters
    {
        return $this->sshParameters;
    }

    public static function fromRaw(array $databaseParameters): DatabaseParameters
    {
        return new DatabaseParameters($databaseParameters);
    }
}
