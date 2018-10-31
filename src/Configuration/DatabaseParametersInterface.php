<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Configuration;

interface DatabaseParametersInterface
{
    public function getHost(): string;

    public function getPort(): int;

    public function getDatabase(): ?string;

    public function getUser(): string;

    public function getPassword(): string;

    public function getSsh(): ?SshParametersInterface;
}
