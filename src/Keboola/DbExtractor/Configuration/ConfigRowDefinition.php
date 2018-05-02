<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

class ConfigRowDefinition implements ConfigurationInterface
{
    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder();
        $rootNode = $treeBuilder->root('parameters');

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
            ->arrayNode('db')
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
            ->scalarNode('password')->end()
            ->scalarNode('#password')->end()
            ->append($this->addSshNode())
            ->end()
            ->end()
            ->integerNode('id')
            ->isRequired()
            ->min(0)
            ->end()
            ->scalarNode('name')
            ->isRequired()
            ->cannotBeEmpty()
            ->end()
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
            ->min(1)
            ->end()
            ->end();
        return $treeBuilder;
    }

    /**
     * @return ArrayNodeDefinition|NodeDefinition
     */
    public function addSshNode()
    {
        $builder = new TreeBuilder();
        $node = $builder->root('ssh');

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
        return $node;
    }
}
