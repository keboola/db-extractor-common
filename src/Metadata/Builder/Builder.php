<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Metadata\Builder;

use Keboola\DbExtractor\ValueObject;

interface Builder
{
    public static function create(): self;

    public function build(): ValueObject;
}
