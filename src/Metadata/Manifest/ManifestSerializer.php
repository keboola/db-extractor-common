<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Metadata\Manifest;

use Keboola\DbExtractor\Metadata\ValueObject\Column;
use Keboola\DbExtractor\Metadata\ValueObject\Table;

interface ManifestSerializer
{
    public function serializeTable(Table $table): array;

    public function serializeColumn(Column $column): array;
}
