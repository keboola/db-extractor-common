<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

class ConfigRowDefinition implements ConfigurationInterface
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
                ->append($this->addDbNode())
                ->integerNode('id')
                    ->min(0)
                ->end()
                ->scalarNode('name')->end()
                ->scalarNode('query')->end()
                ->arrayNode('table')
                    ->children()
                        ->scalarNode('schema')->end()
                        ->scalarNode('tableName')->end()
                    ->end()
                ->end()
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
        // @formatter:on

        // Defined Query and Table node at the same time
        $rootNode->validate()
            ->ifTrue(function ($v) {
                return (isset($v['query']) && isset($v['table']));
            })
            ->thenInvalid('Both table and query cannot be set together.')
            ->end();

        // Undefined Query and Table node
        $rootNode->validate()
            ->ifTrue(function ($v) {
                return (!isset($v['query']) && !isset($v['table']));
            })
            ->thenInvalid('One of table or query is required.')
            ->end();

        // Defined Table node contains required fields
        $rootNode->validate()
            ->ifTrue(function ($v) {
                return (
                    !isset($v['query'])
                    && (!isset($v['table']['tableName'])
                        || !isset($v['table']['schema'])
                        || $v['table']['tableName'] === '')
                );
            })
            ->thenInvalid('The table property requires "tableName" and "schema"')
            ->end();

        // Defined Incremental Fetching without Table node (advanced query)
        $rootNode->validate()
            ->ifTrue(function ($v) {
                return (!isset($v['table']) && isset($v['incrementalFetchingColumn']));
            })
            ->thenInvalid('Incremental fetching is not supported for advanced query.')
            ->end();

        return $treeBuilder;
    }

    public function addDbNode(): ArrayNodeDefinition
    {
        $builder = new TreeBuilder();

        /** @var ArrayNodeDefinition $node */
        $node = $builder->root('db');

        // @formatter:off
        $node
            ->children()
                ->scalarNode('driver')->end()
                ->scalarNode('host')->end()
                ->scalarNode('port')->end()
                ->scalarNode('database')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('user')
                    ->isRequired()
                ->end()
                ->scalarNode('#password')->end()
                ->append($this->addSshNode())
            ->end();
        // @formatter:on

        return $node;
    }

    public function addSshNode(): ArrayNodeDefinition
    {
        $builder = new TreeBuilder();

        /** @var ArrayNodeDefinition */
        $node = $builder->root('ssh');

        // @formatter:off
        $node
            ->children()
                ->booleanNode('enabled')->end()
                ->arrayNode('keys')
                    ->children()
                        ->scalarNode('private')->end()
                        ->scalarNode('#private')->end()
                        ->scalarNode('public')->end()
                    ->end()
                ->end()
                ->scalarNode('sshHost')->end()
                ->scalarNode('sshPort')->end()
                ->scalarNode('remoteHost')->end()
                ->scalarNode('remotePort')->end()
                ->scalarNode('localPort')->end()
                ->scalarNode('user')->end()
            ->end();
        // @formatter:on

        return $node;
    }
}
