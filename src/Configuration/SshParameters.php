<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Configuration;

class SshParameters
{
    /** @var bool|null */
    private $enabled;

    /** @var array|null */
    private $keys;

    /** @var string|null */
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

    public function __construct(
        ?bool $enabled = false,
        ?array $keys = null,
        ?string $sshHost = null,
        ?int $sshPort = 22,
        ?string $remoteHost = null,
        ?int $remotePort = null,
        ?int $localPort = 33006,
        ?string $user = null
    ) {
        $this->enabled = $enabled;
        $this->keys = $keys;
        $this->sshHost = $sshHost;
        $this->sshPort = $sshPort;
        $this->remoteHost = $remoteHost;
        $this->remotePort = $remotePort;
        $this->localPort = $localPort;
        $this->user = $user;
    }

    public function getEnabled(): bool
    {
        return $this->enabled;
    }

    public function getKeys(): ?array
    {
        return $this->keys;
    }

    public function getSshHost(): ?string
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
        return new SshParameters(
            $sshParameters['enabled'] ?? null,
            $sshParameters['keys'] ?? null,
            $sshParameters['sshHost'] ?? null,
            isset($sshParameters['sshPort']) ? (int) $sshParameters['sshPort'] : null,
            $sshParameters['remoteHost'] ?? null,
            isset($sshParameters['remotePort']) ? (int) $sshParameters['remotePort'] : null,
            isset($sshParameters['localPort']) ? (int) $sshParameters['localPort'] : null,
            $sshParameters['user'] ?? null
        );
    }
}
