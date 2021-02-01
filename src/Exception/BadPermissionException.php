<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Exception;

use Exception;
use Keboola\CommonExceptions\UserExceptionInterface;

class BadPermissionException extends Exception implements UserExceptionInterface
{

}
