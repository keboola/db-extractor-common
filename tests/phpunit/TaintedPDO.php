<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

class TaintedPDO extends \PDO
{
    /**
     * @var callable
     */
    private $callback;

    public function setOnEvent(callable $onEvent): void
    {
        $this->callback = $onEvent;
    }

    /**
     * @param string $statement
     * @param array $options
     * @return bool|\PDOStatement
     */
    public function prepare($statement, $options = array())
    {
        call_user_func($this->callback, 'prepare');
        return parent::prepare($statement, $options);
    }

    /**
     * @param string $statement
     * @param null $mode
     * @param null $arg3
     * @param array $ctorargs
     * @return false|\PDOStatement
     */
    public function query($statement, $mode = null, $arg3 = null, array $ctorargs = [])
    {
        call_user_func($this->callback, 'query');
        if ($mode !== null) {
            $ret = parent::query($statement, $mode, $arg3, $ctorargs);
        } elseif (is_int($arg3)) {
            $ret = parent::query($statement, \PDO::FETCH_COLUMN, $arg3);
        } elseif (is_string($arg3)) {
            $ret = parent::query($statement, \PDO::FETCH_CLASS, $arg3, $ctorargs);
        } elseif (is_object($arg3)) {
            $ret = parent::query($statement, \PDO::FETCH_INTO, $arg3);
        } else {
            $ret = parent::query($statement);
        }
        return $ret;
    }
}
