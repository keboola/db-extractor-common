<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Configuration;

use Keboola\Component\Config\BaseConfig;

class BaseExtractorConfig extends BaseConfig
{
    /** @var DatabaseParameters|null */
    private $databaseParameters;

    public function getDbParameters(): DatabaseParameters
    {
        if ($this->databaseParameters) {
            return $this->databaseParameters;
        }

        return DatabaseParameters::fromRaw($this->getValue(['parameters', 'db']));
    }

    public function getSshParameters(): ?SshParameters
    {
        $ssh = $this->getValue(['parameters', 'db', 'ssh'], false);

        if (!$ssh) {
            return null;
        }
        return SshParameters::fromRaw($ssh);
    }

    public function getConfigRowTableParameters(): ?TableParameters
    {
        if (!$this->isConfigRow()) {
            return null;
        }
        return TableParameters::fromRaw($this->getParameters());
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

    public function setDbParameters(DatabaseParameters $databaseParameters): void
    {
        $this->databaseParameters = $databaseParameters;
    }
}
