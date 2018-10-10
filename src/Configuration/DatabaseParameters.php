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

    public function __construct(
        string $host,
        string $user,
        string $password,
        ?string $database = null,
        ?int $port = null
    ) {
        $this->host = $host;
        $this->port = $port;
        $this->database = $database;
        $this->user = $user;
        $this->password = $password;
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
        return new DatabaseParameters(
            $databaseParameters['host'],
            $databaseParameters['user'],
            $databaseParameters['#password'],
            $databaseParameters['database'] ?? null,
            isset($databaseParameters['port']) ? (int) $databaseParameters['port'] : null
        );
    }
}
