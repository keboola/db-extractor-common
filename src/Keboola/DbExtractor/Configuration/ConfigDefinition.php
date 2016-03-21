<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 09/03/16
 * Time: 11:23
 */
namespace Keboola\DbExtractor\Configuration;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

class ConfigDefinition implements ConfigurationInterface
{
    /**
     * Generates the configuration tree builder.
     *
     * @return \Symfony\Component\Config\Definition\Builder\TreeBuilder The tree builder
     */
    public function getConfigTreeBuilder()
    {
        $treeBuilder = new TreeBuilder();
        $rootNode = $treeBuilder->root('config');

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
                ->arrayNode('parameters')
                    ->children()
                        ->arrayNode('db')
                            ->children()
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
                        ->arrayNode('tables')
                            ->isRequired()
                            ->prototype('array')
                                ->children()
                                    ->integerNode('id')
                                        ->isRequired()
                                        ->min(0)
                                    ->end()
                                    ->scalarNode('name')
                                        ->isRequired()
                                        ->cannotBeEmpty()
                                    ->end()
                                    ->scalarNode('query')
                                        ->isRequired()
                                        ->cannotBeEmpty()
                                    ->end()
                                    ->scalarNode('outputTable')
                                        ->isRequired()
                                        ->cannotBeEmpty()
                                    ->end()
                                    ->booleanNode('incremental')
                                        ->defaultValue(false)
                                    ->end()
                                    ->booleanNode('enabled')
                                        ->defaultValue(true)
                                    ->end()
                                    ->scalarNode('primaryKey')
                                        ->defaultValue(null)
                                    ->end()
                                    ->integerNode('retries')
                                        ->min(1)
                                    ->end()
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
                ->arrayNode('image_parameters')
                ->end()
            ->end()
        ;

        return $treeBuilder;
    }

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
            ->end()
        ;

        return $node;
    }
}
