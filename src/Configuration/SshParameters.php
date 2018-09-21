<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Configuration;

class SshParameters
{
    /** @var bool */
    private $enabled;

    /** @var string */
    private $sshHost;

    /** @var int */
    private $sshPort;

    /** @var string */
    private $remoteHost;

    /** @var int */
    private $remotePort;

    /** @var int  */
    private $localPort;

    /** @var string */
    private $user;

    /** @var string */
    private $privateKey;

    /** @var bool */
    private $compression;

    public function __construct(
        bool $enabled,
        string $sshHost,
        int $sshPort,
        string $user,
        string $privateKey,
        string $databaseHost,
        int $databasePort,
        int $localPort,
        bool $compression
    ) {
        $this->enabled = $enabled;
        $this->privateKey = $privateKey;
        $this->sshHost = $sshHost;
        $this->sshPort = $sshPort;
        $this->remoteHost = $databaseHost;
        $this->remotePort = $databasePort;
        $this->localPort = $localPort;
        $this->user = $user;
        $this->compression = $compression;
    }

    public function isEnabled(): bool
    {
        return $this->enabled;
    }

    public function getSshHost(): string
    {
        return $this->sshHost;
    }

    public function getSshPort(): int
    {
        return $this->sshPort;
    }

    public function getRemoteHost(): string
    {
        return $this->remoteHost;
    }

    public function getRemotePort(): int
    {
        return $this->remotePort;
    }

    public function getLocalPort(): int
    {
        return $this->localPort;
    }

    public function getUser(): string
    {
        return $this->user;
    }

    public function getPrivateKey(): string
    {
        return $this->privateKey;
    }

    private function getCompression(): bool
    {
        return $this->compression;
    }

    public function toArray(): array
    {
        return [
            'user' => $this->getUser(),
            'sshHost' => $this->getSshHost(),
            'sshPort' => $this->getSshPort(),
            'localPort' => $this->getLocalPort(),
            'remoteHost' => $this->getRemoteHost(),
            'remotePort' => $this->getRemotePort(),
            'privateKey' => $this->getPrivateKey(),
            'compression' => $this->getCompression(),
        ];
    }

    public static function fromRaw(
        array $sshParameters,
        string $databaseHost,
        int $databasePort,
        string $databaseUser
    ): SshParameters {
        return new SshParameters(
            $sshParameters['enabled'],
            $sshParameters['sshHost'],
            (int) $sshParameters['sshPort'],
            $sshParameters['user'] ?? $databaseUser,
            $sshParameters['keys']['#private'],
            $databaseHost,
            $databasePort,
            (int) $sshParameters['localPort'],
            (bool) $sshParameters['compression']
        );
    }
}
