<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Configuration;

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

    public function __construct(array $databaseParameters)
    {
        $this->host = $databaseParameters['host'];
        $this->port = $databaseParameters['port'] ?? null;
        $this->database = $databaseParameters['database'] ?? null;
        $this->user = $databaseParameters['user'];
        $this->password = $databaseParameters['#password'];
    }

    public function getHost(): string
    {
        return $this->host;
    }

    public function getPort(): ?int
    {
        return (int) $this->port;
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

    public static function fromRaw(array $databaseParameters): DatabaseParameters
    {
        return new DatabaseParameters($databaseParameters);
    }
}
