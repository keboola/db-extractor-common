<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Configuration;

interface SshParametersInterface
{
    public function isEnabled(): bool;

    public function getKeys(): ?array;

    public function getSshHost(): ?string;

    public function getRemoteHost(): string;

    public function getRemotePort(): ?int;

    public function getLocalPort(): int;

    public function getUser(): string;

    public function getPrivateKey(): ?string;

    public function toArray(): array;
}
