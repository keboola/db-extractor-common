<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Configuration\Definition;

use Keboola\DbExtractorCommon\Configuration\ConfigDefinitionValidationHelper;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class ConfigRowDefinition extends BaseExtractorConfigDefinition
{
    public function getParametersDefinition(): ArrayNodeDefinition
    {
        $rootNode = parent::getParametersDefinition();

        // @formatter:off
        $rootNode
            ->children()
                ->append($this->getDbNode())
                ->integerNode('id')->end()
                ->scalarNode('name')->end()
                ->scalarNode('query')->end()
                ->append($this->getTableNode())
                ->arrayNode('columns')
                    ->prototype('scalar')->end()
                ->end()
                ->scalarNode('outputTable')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->booleanNode('incremental')
                    ->defaultValue(false)
                ->end()
                ->scalarNode('incrementalFetchingColumn')->end()
                ->scalarNode('incrementalFetchingLimit')->end()
                ->booleanNode('enabled')
                    ->defaultValue(true)
                ->end()
                ->arrayNode('primaryKey')
                    ->prototype('scalar')->end()
                ->end()
                ->integerNode('retries')
                    ->min(0)
                ->end()
            ->end();

        $rootNode->validate()
            ->ifTrue(function ($v) {
                return ConfigDefinitionValidationHelper::isNeitherQueryNorTableDefined($v);
            })
            ->thenInvalid(ConfigDefinitionValidationHelper::MESSAGE_TABLE_OR_QUERY_MUST_BE_DEFINED)
            ->end();

        $rootNode->validate()
            ->ifTrue(function ($v) {
                return ConfigDefinitionValidationHelper::areBothQueryAndTableSet($v);
            })
            ->thenInvalid(ConfigDefinitionValidationHelper::MESSAGE_TABLE_AND_QUERY_CANNOT_BE_SET_TOGETHER)
            ->end();

        $rootNode->validate()
            ->ifTrue(function ($v) {
                return ConfigDefinitionValidationHelper::isIncrementalFetchingSetForAdvancedQuery($v);
            })
            ->thenInvalid(ConfigDefinitionValidationHelper::MESSAGE_CUSTOM_QUERY_CANNOT_BE_FETCHED_INCREMENTALLY)
            ->end();
        // @formatter:on

        return $rootNode;
    }
}
