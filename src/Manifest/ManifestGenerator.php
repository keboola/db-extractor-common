<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Manifest;

use Keboola\DbExtractor\Adapter\ValueObject\ExportResult;
use Keboola\DbExtractorConfig\Configuration\ValueObject\ExportConfig;

interface ManifestGenerator
{
    public function generate(ExportConfig $exportConfig, ExportResult $exportResult, bool $legacy): array;
}
