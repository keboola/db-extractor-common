<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

class SshParameters
{
    /** @var bool|null */
    private $enabled;

    /** @var array|null */
    private $keys;

    /** @var string */
    private $sshHost;

    /** @var int */
    private $sshPort;

    /** @var string|null */
    private $remoteHost;

    /** @var int|null */
    private $remotePort;

    /** @var int  */
    private $localPort;

    /** @var string|null */
    private $user;

    /** @var string|null */
    private $privateKey;

    public function __construct(array $sshParameters)
    {
        $this->enabled = $sshParameters['enabled'] ?? null;
        $this->keys = $sshParameters['keys'] ?? null;
        $this->sshHost = $sshParameters['sshHost'];
        $this->sshPort = $sshParameters['sshPort'] ? (int) $sshParameters['sshPort'] : 22;
        $this->remoteHost = $sshParameters['remoteHost'] ?? null;
        $this->remotePort = $sshParameters['remotePort'] ?? null;
        $this->localPort = $sshParameters['localPort'] ? (int) $sshParameters['localPort'] : 33006;
        $this->user = $sshParameters['user'] ?? null;
    }

    public function getEnabled(): ?bool
    {
        return $this->enabled;
    }

    public function getKeys(): ?array
    {
        return $this->keys;
    }

    public function getSshHost(): string
    {
        return $this->sshHost;
    }

    public function getSshPort(): int
    {
        return $this->sshPort;
    }

    public function getRemoteHost(): ?string
    {
        return $this->remoteHost;
    }

    public function getRemotePort(): ?int
    {
        return (int) $this->remotePort;
    }

    public function getLocalPort(): int
    {
        return $this->localPort;
    }

    public function getUser(): ?string
    {
        return $this->user;
    }

    public function getPrivateKey(): ?string
    {
        return $this->privateKey;
    }

    public function setRemoteHost(?string $remoteHost): void
    {
        $this->remoteHost = $remoteHost;
    }

    public function setRemotePort(?int $remotePort): void
    {
        $this->remotePort = $remotePort;
    }

    public function setUser(?string $user): void
    {
        $this->user = $user;
    }

    public function setPrivateKey(?string $privateKey): void
    {
        $this->privateKey = $privateKey;
    }

    public function toArray(): array
    {
        // 'remotePort', 'privateKey',
        return [
            'user' => $this->getUser(),
            'sshHost' => $this->getSshHost(),
            'sshPort' => $this->getSshPort(),
            'localPort' => $this->getLocalPort(),
            'remoteHost' => $this->getRemoteHost(),
            'remotePort' => $this->getRemotePort(),
            'privateKey' => $this->getPrivateKey(),
        ];
    }

    public static function fromRaw(array $sshParameters): SshParameters
    {
        return new SshParameters($sshParameters);
    }
}
