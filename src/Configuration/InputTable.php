<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Keboola\DbExtractor\Exception\InvalidArgumentException;
use Keboola\DbExtractor\ValueObject;

class InputTable implements ValueObject
{
    private string $name;

    private string $schema;

    public static function fromArray(array $data): self
    {
        if (!isset($data['table'])) {
            throw new InvalidArgumentException('Key "table" is required.');
        }
        $table = $data['table'];

        if (!isset($table['tableName'])) {
            throw new InvalidArgumentException('Key "table.tableName" is required.');
        }

        if (!isset($table['schema'])) {
            throw new InvalidArgumentException('Key "table.schema" is required.');
        }

        return new self($table['tableName'], $table['schema']);
    }

    public function __construct(string $name, string $schema)
    {
        if ($name === '') {
            throw new InvalidArgumentException('Name cannot be empty string.');
        }

        if ($schema === '') {
            throw new InvalidArgumentException('Schema cannot be empty string.');
        }

        $this->name = $name;
        $this->schema = $schema;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getSchema(): string
    {
        return $this->schema;
    }
}
