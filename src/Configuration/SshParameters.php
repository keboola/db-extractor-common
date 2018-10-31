<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Configuration;

class SshParameters implements SshParametersInterface
{
    /** @var bool */
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
        bool $enabled,
        string $databaseHost,
        ?int $databasePort,
        ?array $keys = null,
        ?string $sshHost = null,
        ?int $sshPort = 22,
        ?int $localPort = 33006,
        ?string $user = null
    ) {
        $this->enabled = $enabled;
        $this->keys = $keys;
        $this->privateKey = $this->getKeys()['#private'] ?? $this->getKeys()['private'];
        $this->sshHost = $sshHost;
        $this->sshPort = $sshPort;
        $this->remoteHost = $databaseHost;
        $this->remotePort = $databasePort;
        $this->localPort = $localPort;
        $this->user = $user;
    }

    public function isEnabled(): bool
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

    public function getRemoteHost(): string
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

    public function getUser(): string
    {
        return $this->user;
    }

    public function getPrivateKey(): ?string
    {
        return $this->privateKey;
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

    public static function fromRaw(
        array $sshParameters,
        string $databaseHost,
        ?int $databasePort,
        string $databaseUser
    ): SshParameters {
        return new SshParameters(
            $sshParameters['enabled'] ?? false,
            $databaseHost,
            $databasePort,
            $sshParameters['keys'] ?? null,
            $sshParameters['sshHost'] ?? null,
            isset($sshParameters['sshPort']) ? (int) $sshParameters['sshPort'] : null,
            isset($sshParameters['localPort']) ? (int) $sshParameters['localPort'] : null,
            $sshParameters['user'] ?? $databaseUser
        );
    }
}
