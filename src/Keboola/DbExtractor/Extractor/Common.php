<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\DeadConnectionException;
use Keboola\DbExtractor\Exception\UserException;
use PDO;

class Common extends Extractor
{
    public const TYPE_AUTO_INCREMENT = 'autoIncrement';
    public const TYPE_TIMESTAMP = 'timestamp';

    /** @var array */
    protected $database;

    public function createConnection(array $params): PDO
    {
        // convert errors to PDOExceptions
        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ];

        // check params
        foreach (['host', 'database', 'user', 'password'] as $r) {
            if (!isset($params[$r])) {
                throw new UserException(sprintf("Parameter %s is missing.", $r));
            }
        }

        $this->database = $params['database'];

        $port = isset($params['port']) ? $params['port'] : '3306';
        $dsn = sprintf("mysql:host=%s;port=%s;dbname=%s;charset=utf8", $params['host'], $port, $params['database']);

        $pdo = new PDO($dsn, $params['user'], $params['password'], $options);
        $pdo->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
        $pdo->exec("SET NAMES utf8;");
        return $pdo;
    }

    public function testConnection(): void
    {
        $this->db->query("SELECT 1");
    }

    /**
     * @param array $table
     * @param string $columnName
     * @param int $limit
     * @throws UserException
     */
    public function validateIncrementalFetching(array $table, string $columnName, ?int $limit = null): void
    {
        $res = $this->db->query(
            sprintf(
                'SELECT * FROM INFORMATION_SCHEMA.COLUMNS as cols 
                            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s',
                $this->db->quote($table['schema']),
                $this->db->quote($table['tableName']),
                $this->db->quote($columnName)
            )
        );
        $columns = $res->fetchAll();
        if (count($columns) === 0) {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching was not found in the table',
                    $columnName
                )
            );
        }
        if ($columns[0]['EXTRA'] === 'auto_increment') {
            $this->incrementalFetching['column'] = $columnName;
            $this->incrementalFetching['type'] = self::TYPE_AUTO_INCREMENT;
        } else if ($columns[0]['EXTRA'] === 'on update CURRENT_TIMESTAMP' && $columns[0]['COLUMN_DEFAULT'] === 'CURRENT_TIMESTAMP') {
            $this->incrementalFetching['column'] = $columnName;
            $this->incrementalFetching['type'] = self::TYPE_TIMESTAMP;
        } else {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching is not an auto increment column or an auto update timestamp',
                    $columnName
                )
            );
        }
        if ($limit) {
            $this->incrementalFetching['limit'] = $limit;
        }
    }

    public function simpleQuery(array $table, array $columns = array()): string
    {
        $incrementalAddon = null;
        if ($this->incrementalFetching && isset($this->state['lastFetchedRow'])) {
            if ($this->incrementalFetching['type'] === self::TYPE_AUTO_INCREMENT) {
                $incrementalAddon = sprintf(
                    ' %s > %d',
                    $this->quote($this->incrementalFetching['column']),
                    (int) $this->state['lastFetchedRow']
                );
            } else if ($this->incrementalFetching['type'] === self::TYPE_AUTO_INCREMENT) {
                $incrementalAddon = sprintf(
                    " %s > '%s'",
                    $this->quote($this->incrementalFetching['column']),
                    $this->state['lastFetchedRow']
                );
            } else {
                throw new ApplicationException(
                    sprintf('Unknown incremental fetching column type %s', $this->incrementalFetching['type'])
                );
            }
        }
        if (count($columns) > 0) {
            $query = sprintf(
                "SELECT %s FROM %s.%s",
                implode(
                    ', ',
                    array_map(
                        function ($column) {
                            return $this->quote($column);
                        },
                        $columns
                    )
                ),
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        } else {
            $query = sprintf(
                "SELECT * FROM %s.%s",
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        }

        if ($incrementalAddon) {
            $query .= sprintf(
                " WHERE %s ORDER BY %s",
                $incrementalAddon,
                $this->quote($this->incrementalFetching['column'])
            );
        }
        if (isset($this->incrementalFetching['limit'])) {
            $query .= sprintf(
                " LIMIT %d",
                $this->incrementalFetching['limit']
            );
        }
        return $query;
    }

    public function getTables(?array $tables = null): array
    {

        $sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES as c";

        $whereClause = " WHERE c.TABLE_SCHEMA != 'performance_schema' 
                          AND c.TABLE_SCHEMA != 'mysql'
                          AND c.TABLE_SCHEMA != 'information_schema'";

        if ($this->database) {
            $whereClause = sprintf(" WHERE c.TABLE_SCHEMA = %s", $this->db->quote($this->database));
        }

        if (!is_null($tables) && count($tables) > 0) {
            $whereClause .= sprintf(
                " AND c.TABLE_NAME IN (%s) AND c.TABLE_SCHEMA IN (%s)",
                implode(
                    ',',
                    array_map(
                        function ($table) {
                            return $this->db->quote($table['tableName']);
                        },
                        $tables
                    )
                ),
                implode(
                    ',',
                    array_map(
                        function ($table) {
                            return $this->db->quote($table['schema']);
                        },
                        $tables
                    )
                )
            );
        }

        $sql .= $whereClause;

        $sql .= " ORDER BY TABLE_SCHEMA, TABLE_NAME";

        $res = $this->db->query($sql);
        $arr = $res->fetchAll(PDO::FETCH_ASSOC);
        if (count($arr) === 0) {
            return [];
        }

        $tableNameArray = [];
        $tableDefs = [];

        foreach ($arr as $table) {
            $tableNameArray[] = $table['TABLE_NAME'];
            $tableDefs[$table['TABLE_SCHEMA'] . '.' . $table['TABLE_NAME']] = [
                'name' => $table['TABLE_NAME'],
                'sanitizedName' => \Keboola\Utils\sanitizeColumnName($table['TABLE_NAME']),
                'schema' => (isset($table['TABLE_SCHEMA'])) ? $table['TABLE_SCHEMA'] : '',
                'type' => (isset($table['TABLE_TYPE'])) ? $table['TABLE_TYPE'] : '',
                'rowCount' => (isset($table['TABLE_ROWS'])) ? $table['TABLE_ROWS'] : '',
            ];
            if ($table["AUTO_INCREMENT"]) {
                $tableDefs[$table['TABLE_SCHEMA'] . '.' . $table['TABLE_NAME']]['autoIncrement'] = $table['AUTO_INCREMENT'];
            }
        }

        if (!is_null($tables) && count($tables) > 0) {
            $sql = "SELECT c.*, 
                    CONSTRAINT_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME, REFERENCED_TABLE_SCHEMA
                    FROM INFORMATION_SCHEMA.COLUMNS as c 
                    LEFT OUTER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE as kcu
                    ON c.TABLE_NAME = kcu.TABLE_NAME AND c.COLUMN_NAME = kcu.COLUMN_NAME";
        } else {
            $sql = "SELECT c.*
                    FROM INFORMATION_SCHEMA.COLUMNS as c";
        }

        $sql .= $whereClause;

        $sql .= " ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, ORDINAL_POSITION";

        $res = $this->db->query($sql);
        $rows = $res->fetchAll(PDO::FETCH_ASSOC);

        foreach ($rows as $i => $column) {
            $curTable = $column['TABLE_SCHEMA'] . '.' . $column['TABLE_NAME'];
            $length = ($column['CHARACTER_MAXIMUM_LENGTH']) ? $column['CHARACTER_MAXIMUM_LENGTH'] : null;
            if (is_null($length) && !is_null($column['NUMERIC_PRECISION'])) {
                if ($column['NUMERIC_SCALE'] > 0) {
                    $length = $column['NUMERIC_PRECISION'] . "," . $column['NUMERIC_SCALE'];
                } else {
                    $length = $column['NUMERIC_PRECISION'];
                }
            }
            $curColumn = [
                "name" => $column['COLUMN_NAME'],
                "sanitizedName" => \Keboola\Utils\sanitizeColumnName($column['COLUMN_NAME']),
                "type" => $column['DATA_TYPE'],
                "primaryKey" => ($column['COLUMN_KEY'] === "PRI") ? true : false,
                "length" => $length,
                "nullable" => ($column['IS_NULLABLE'] === "NO") ? false : true,
                "default" => $column['COLUMN_DEFAULT'],
                "ordinalPosition" => $column['ORDINAL_POSITION'],
            ];

            if (array_key_exists('CONSTRAINT_NAME', $column) && !is_null($column['CONSTRAINT_NAME'])) {
                $curColumn['constraintName'] = $column['CONSTRAINT_NAME'];
            }
            if (array_key_exists('REFERENCED_TABLE_NAME', $column) && !is_null($column['REFERENCED_TABLE_NAME'])) {
                $curColumn['foreignKeyRefSchema'] = $column['REFERENCED_TABLE_SCHEMA'];
                $curColumn['foreignKeyRefTable'] = $column['REFERENCED_TABLE_NAME'];
                $curColumn['foreignKeyRefColumn'] = $column['REFERENCED_COLUMN_NAME'];
            }
            if ($column['EXTRA']) {
                $curColumn["extra"] = $column["EXTRA"];
                if ($column['EXTRA'] === 'auto_increment') {
                    $curColumn['autoIncrement'] = $tableDefs[$curTable]['autoIncrement'];
                }
                if ($column['EXTRA'] === 'on update CURRENT_TIMESTAMP' && $column['COLUMN_DEFAULT'] === 'CURRENT_TIMESTAMP') {
                    $tableDefs[$curTable]['timestampUpdateColumn'] = $column['COLUMN_NAME'];
                }
            }
            $tableDefs[$curTable]['columns'][$column['ORDINAL_POSITION'] - 1] = $curColumn;
        }
        return array_values($tableDefs);
    }

    private function quote(string $obj): string
    {
        return "`{$obj}`";
    }
}
