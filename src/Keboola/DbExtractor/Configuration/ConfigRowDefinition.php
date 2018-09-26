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
                ->append($this->getDbParametersDefinition())
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
                return (!isset($v['query']) && !isset($v['table']));
            })
            ->thenInvalid('Either "table" or "query" must be defined.')
            ->end();

        $rootNode->validate()
            ->ifTrue(function ($v) {
                return (isset($v['query']) && isset($v['table']));
            })
            ->thenInvalid('Both "table" and "query" cannot be set together.')
            ->end();

        $rootNode->validate()
            ->ifTrue(function ($v) {
                return (isset($v['query']) && isset($v['incrementalFetchingColumn']));
            })
            ->thenInvalid('Incremental fetching is not supported for advanced queries.')
            ->end();
        // @formatter:on

        return $treeBuilder;
    }
}
