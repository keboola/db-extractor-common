<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class ConfigRowDefinition extends BaseExtractorConfigDefinition
{
    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder();
        /** @var ArrayNodeDefinition */
        $rootNode = $treeBuilder->root('parameters');

        // @formatter:off
        $rootNode
            ->children()
                ->scalarNode('data_dir')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('extractor_class')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
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

        return $treeBuilder;
    }
}
