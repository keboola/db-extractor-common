<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

abstract class BaseExtractorConfigDefinition implements ConfigurationInterface
{
    abstract public function getConfigTreeBuilder(): TreeBuilder;

    protected function getDbParametersDefinition(): ArrayNodeDefinition
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
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('user')
                    ->isRequired()
                ->end()
                ->scalarNode('#password')
                    ->isRequired()
                ->end()
                ->append($this->getSshParametersNode());
            //->end();
        // @formatter:on

        return $node;
    }

    protected function getSshParametersNode(): ArrayNodeDefinition
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
                ->scalarNode('user')->end();
            //->end();
        // @formatter:on

        return $node;
    }

    protected function getTableNode(): ArrayNodeDefinition
    {
        $builder = new TreeBuilder();

        /** @var ArrayNodeDefinition $node */
        $node = $builder->root('table');

        // @formatter:off
        $node
            ->children()
                ->scalarNode('schema')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('tableName')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
            ->end();
        // @formatter:on

        return $node;
    }

    protected function getTablesNode(): ArrayNodeDefinition
    {
        $builder = new TreeBuilder();

        /** @var ArrayNodeDefinition $node */
        $node = $builder->root('tables');

        // @formatter:off
        $node
            ->arrayPrototype()
            ->addDefaultsIfNotSet()
                ->children()
                    ->integerNode('id')
                        ->isRequired()
                        ->min(0)
                    ->end()
                    ->scalarNode('name')
                        ->isRequired()
                        ->cannotBeEmpty()
                    ->end()
                    ->scalarNode('query')->end()
                    ->arrayNode('columns')
                        ->prototype('scalar')->end()
                    ->end()
                    ->scalarNode('outputTable')
                        ->isRequired()
                        ->cannotBeEmpty()
                    ->end()
                    ->append($this->getTableNode())
                    ->booleanNode('incremental')
                        ->defaultValue(false)
                    ->end()
                    ->booleanNode('enabled')
                        ->defaultValue(true)
                    ->end()
                    ->arrayNode('primaryKey')
                        ->prototype('scalar')->end()
                    ->end()
                    ->integerNode('retries')
                        ->min(0)
                    ->end()
                ->end()
            ->end();

        $node->validate()
            ->ifTrue(function ($v) {
                foreach ($v as $table) {
                    return (!isset($table['query']) && !isset($table['table']));
                }
            })
            ->thenInvalid('Either "table" or "query" must be defined.')
            ->end();

        $node->validate()
            ->ifTrue(function ($v) {
                foreach ($v as $table) {
                    return (isset($table['query']) && isset($table['table']));
                }
            })
            ->thenInvalid('Both "table" and "query" cannot be set together.')
            ->end();

        $node->validate()
            ->ifTrue(function ($v) {
                foreach ($v as $table) {
                    return (isset($table['query']) && isset($table['incrementalFetchingColumn']));
                }
            })
            ->thenInvalid('Incremental fetching is not supported for advanced queries.')
            ->end();
        // @formatter:on

        return $node;
    }
}
