<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

class TaintedPDO extends \PDO
{
    /**
     * @var callable
     */
    private $callback;

    public function setOnEvent($onEvent)
    {
        $this->callback = $onEvent;
    }

    public function query($statement, $mode = null, $arg3 = null, array $ctorargs = [])
    {
        fwrite(STDERR, sprintf('[%s] Querying ' . $statement, date('Y-m-d H:i:s')) . PHP_EOL);

        $stmt = parent::query('SELECT CONNECTION_ID() AS pid;');
        $stmt->execute();
        fwrite(STDERR, sprintf('[%s] Using pid ' . $stmt->fetchAll()[0]['pid'], date('Y-m-d H:i:s')) . PHP_EOL);

        call_user_func($this->callback, 'query', null, $this);

        $stmt = parent::query('SELECT CONNECTION_ID() AS pid;');
        $stmt->execute();
        fwrite(STDERR, sprintf('[%s] Using pid ' . $stmt->fetchAll()[0]['pid'], date('Y-m-d H:i:s')) . PHP_EOL);


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
