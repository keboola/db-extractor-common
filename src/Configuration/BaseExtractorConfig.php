<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Configuration;

use Keboola\Component\Config\BaseConfig;

class BaseExtractorConfig extends BaseConfig
{
    public function getDbParameters(): DatabaseParameters
    {
        return DatabaseParameters::fromRaw($this->getValue(['parameters', 'db']));
    }

    /**
     * @return TableParameters[]
     */
    public function getTables(): array
    {
        $tableParameters = [];
        foreach ($this->getValue(['parameters', 'tables']) as $table) {
            $tableParameters[] = TableParameters::fromRaw($table);
        }
        return $tableParameters;
    }

    /**
     * @return TableParameters[]
     */
    public function getEnabledTables(): array
    {
        $tables = [];
        foreach ($this->getTables() as $table) {
            if ($table->isEnabled()) {
                $tables[] = $table;
            }
        }
        return $tables;
    }

    public function isConfigRow(): bool
    {
        return !isset($this->getParameters()['tables']);
    }
}
