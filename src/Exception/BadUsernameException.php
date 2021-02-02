<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Exception;

use Exception;
use Keboola\CommonExceptions\UserExceptionInterface;

class BadUsernameException extends Exception implements UserExceptionInterface
{

}
