<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Configuration\Definition;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class ActionConfigDefinition extends BaseExtractorConfigDefinition
{
    public function getParametersDefinition(): ArrayNodeDefinition
    {
        $rootNode = parent::getParametersDefinition();

        // @formatter:off
        $rootNode
            ->ignoreExtraKeys(false)
            ->children()
                ->append($this->getDbNode())
            ->end();
        // @formatter:on

        return $rootNode;
    }
}
