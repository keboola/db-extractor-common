<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Configuration\Definition;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class ConfigDefinition extends BaseExtractorConfigDefinition
{
    public function getParametersDefinition(): ArrayNodeDefinition
    {
        $rootNode = parent::getParametersDefinition();

        // @formatter:off
        $rootNode
            ->children()
                ->append($this->getDbNode())
                ->append($this->getTablesNode())
            ->end();
        // @formatter:on

        return $rootNode;
    }
}