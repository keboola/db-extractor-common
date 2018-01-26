<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 15/02/16
 * Time: 16:44
 */

$pdo = new \PDO("mysql:host=localhost;dbname=test;charset=utf8", 'travis', '', [
    \PDO::MYSQL_ATTR_LOCAL_INFILE => true
]);

$result = $pdo->exec("
    LOAD DATA LOCAL INFILE '/home/travis/build/keboola/db-extractor-common/tests/data/escaping.csv'
    INTO TABLE escaping
    FIELDS TERMINATED BY ','
    ENCLOSED BY '\"'
    ESCAPED BY ''
    IGNORE 1 LINES
");
