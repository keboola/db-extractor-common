<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\Component\Config\BaseConfig;

class BaseExtractorConfig extends BaseConfig
{
    public function getDbParameters(): array
    {
        return $this->getValue(['parameters', 'db']);
    }
}
