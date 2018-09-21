<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Logger;

class LineFormatter extends \Monolog\Formatter\LineFormatter
{
    /**
     * @param array|string $data
     * @return array|string
     */
    protected function normalize($data)
    {
        return parent::normalize($data);
    }
}
