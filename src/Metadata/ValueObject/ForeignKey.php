<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Metadata\ValueObject;

use Keboola\DbExtractor\Exception\InvalidArgumentException;
use Keboola\DbExtractor\Exception\PropertyNotSetException;
use Keboola\DbExtractor\ValueObject;

class ForeignKey implements ValueObject
{
    private ?string $refSchema;

    private string $refTable;

    private string $refColumn;

    public function __construct(?string $refSchema, string $refTable, string $refColumn)
    {
        if ($refSchema !== null && empty($refSchema)) {
            throw new InvalidArgumentException('Ref schema of foreign key cannot be empty string.');
        }

        if (empty($refTable)) {
            throw new InvalidArgumentException('Ref table cannot be empty.');
        }

        if (empty($refColumn)) {
            throw new InvalidArgumentException('Ref column cannot be empty.');
        }

        $this->refSchema = $refSchema;
        $this->refTable = $refTable;
        $this->refColumn = $refColumn;
    }

    public function hasRefSchema(): bool
    {
        return $this->refSchema !== null;
    }

    public function getRefSchema(): string
    {
        if ($this->refSchema === null) {
            throw new PropertyNotSetException('RefSchema is not set.');
        }

        return $this->refSchema;
    }

    public function getRefTable(): string
    {
        return $this->refTable;
    }

    public function getRefColumn(): string
    {
        return $this->refColumn;
    }
}
