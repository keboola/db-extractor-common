<?php

namespace Keboola\DbExtractorCommon\Tests;

use PDO;

class TaintedPDOStatement extends \PDOStatement
{
    /**
     * @var callable
     */
    private $callback;

    private function __construct($onEvent)
    {
        $this->callback = $onEvent;
    }

    public function fetch($fetch_style = null, $cursor_orientation = PDO::FETCH_ORI_NEXT, $cursor_offset = 0)
    {
        if ($fetch_style === null) {
            $fetch_style = PDO::FETCH_BOTH;
        }
        call_user_func($this->callback, 'fetch');
        return parent::fetch($fetch_style, $cursor_orientation, $cursor_offset);
    }

    public function execute($input_parameters = null)
    {
        call_user_func($this->callback, 'execute');
        return parent::execute($input_parameters);
    }
}
