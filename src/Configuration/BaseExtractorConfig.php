<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Configuration;

use Keboola\Component\Config\BaseConfig;
use Symfony\Component\Config\Definition\ConfigurationInterface;

class BaseExtractorConfig extends BaseConfig
{
    /** @var DatabaseParameters */
    private $dbParameters;

    /** @var SshParameters|null */
    private $sshParameters;

    /**
     * @inheritdoc
     */
    public function __construct($config, ?ConfigurationInterface $configDefinition = null)
    {
        parent::__construct($config, $configDefinition);
        $this->initDbParameters();
    }

    private function initDbParameters(): void
    {
        $databaseParameters = DatabaseParameters::fromRaw($this->getValue(['parameters', 'db']));

        $this->initSshParameters($databaseParameters);
        $sshParameters = $this->getSshParameters();
        if ($sshParameters) {
            $databaseParameters->setHost('127.0.0.1');
            $databaseParameters->setPort($sshParameters->getLocalPort());
        }
        $this->dbParameters = $databaseParameters;
    }

    private function initSshParameters(DatabaseParameters $databaseParameters): void
    {
        $ssh = $this->getValue(['parameters', 'db', 'ssh'], false);
        if (!$ssh) {
            return;
        }

        $sshParameters = SshParameters::fromRaw($databaseParameters, $ssh);
        if (!$sshParameters->isEnabled()) {
            return;
        }

        $this->sshParameters = $sshParameters;
    }

    public function getDbParameters(): DatabaseParameters
    {
        return $this->dbParameters;
    }

    public function getSshParameters(): ?SshParameters
    {
        return $this->sshParameters;
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
}
