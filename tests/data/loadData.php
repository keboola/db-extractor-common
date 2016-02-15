<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 15/02/16
 * Time: 16:44
 */

$pdo = new \PDO("mysql:host=localhost;dbname=test;charset=utf8", 'travis');

$pdo->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
$pdo->setAttribute(\PDO::MYSQL_ATTR_LOCAL_INFILE, true);

$result = $pdo->exec("LOAD DATA LOCAL
    INFILE 'escaping.csv'
    INTO TABLE escaping
    FIELDS TERMINATED BY ','
    ENCLOSED BY '\"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES
");

echo $result;
