<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon;

class IncrementalFetchingSettings
{
    public const TYPE_AUTO_INCREMENT = 'autoIncrement';
    public const TYPE_TIMESTAMP = 'timestamp';

    /** @var string */
    private $column;

    /** @var string */
    private $type;

    /** @var int|null */
    private $limit;

    public function __construct(string $column, string $type)
    {
        $this->column = $column;
        $this->type = $type;
    }

    public function getColumn(): string
    {
        return $this->column;
    }

    public function getType(): string
    {
        return $this->type;
    }

    public function getLimit(): ?int
    {
        return $this->limit;
    }

    public function isTypeAutoIncrement(): bool
    {
        return $this->type === self::TYPE_AUTO_INCREMENT;
    }

    public function isTypeTimestamp(): bool
    {
        return $this->type === self::TYPE_TIMESTAMP;
    }

    public function setLimit(int $limit): void
    {
        $this->limit = $limit;
    }
}
