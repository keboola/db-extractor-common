<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Temp\Temp;

class SslHelper
{
    public static function createSSLFile(Temp $temp, string $fileContent): string
    {
        $filename = $temp->createTmpFile('ssl');
        file_put_contents((string) $filename, $fileContent);
        return (string) $filename->getRealPath();
    }
}
