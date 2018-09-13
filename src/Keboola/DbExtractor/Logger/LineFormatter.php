<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Logger;

class LineFormatter extends \Monolog\Formatter\LineFormatter
{
    /**
     * @param CsvFile|array $data
     * @return array|string
     */
    protected function normalize($data)
    {
        if ($data instanceof CsvFile) {
            return "csv file: " . $data->getFilename();
        } else {
            return parent::normalize($data);
        }
    }
}
