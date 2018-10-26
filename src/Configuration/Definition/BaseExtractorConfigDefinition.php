<?php

declare(strict_types=1);

namespace Keboola\DbExtractorCommon\Configuration\Definition;

use Keboola\Component\Config\BaseConfigDefinition;
use Keboola\DbExtractorCommon\Configuration\ConfigDefinitionValidationHelper;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

abstract class BaseExtractorConfigDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        $parametersNode->validate()
            ->ifTrue(function ($v) {
                $databaseName = $v['db']['database'] ?? null;
                if (isset($v['tables'])) {
                    foreach ($v['tables'] as $tableParameters) {
                        return ConfigDefinitionValidationHelper::isDatabaseAndTableSchemaEqual(
                            $tableParameters,
                            $databaseName
                        );
                    }
                } else {
                    return ConfigDefinitionValidationHelper::isDatabaseAndTableSchemaEqual(
                        $v,
                        $databaseName
                    );
                }
                return false;
            })
            ->thenInvalid(ConfigDefinitionValidationHelper::MESSAGE_DATABASE_AND_TABLE_SCHEMA_ARE_DIFFERENT)
            ->end();
        return $parametersNode;
    }

    protected function getDbNode(): ArrayNodeDefinition
    {
        $builder = new TreeBuilder();

        /** @var ArrayNodeDefinition $node */
        $node = $builder->root('db');

        // @formatter:off
        $node
            ->ignoreExtraKeys(false)
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
                ->append($this->getSshNode());
            //->end();
        // @formatter:on

        return $node;
    }

    protected function getSshNode(): ArrayNodeDefinition
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
                ->scalarNode('sshHost')
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('sshPort')
                    ->defaultValue(22)
                ->end()
                ->scalarNode('remoteHost')->end()
                ->scalarNode('remotePort')->end()
                ->scalarNode('localPort')
                    ->defaultValue(33006)
                ->end()
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
                    ->booleanNode('advancedMode')->end()
                ->end()
            ->end();

        $node->validate()
            ->ifTrue(function ($v) {
                foreach ($v as $table) {
                    return ConfigDefinitionValidationHelper::isNeitherQueryNorTableDefined($table);
                }
            })
            ->thenInvalid(ConfigDefinitionValidationHelper::MESSAGE_TABLE_OR_QUERY_MUST_BE_DEFINED)
            ->end();

        $node->validate()
            ->ifTrue(function ($v) {
                foreach ($v as $table) {
                    return ConfigDefinitionValidationHelper::areBothQueryAndTableSet($table);
                }
            })
            ->thenInvalid(ConfigDefinitionValidationHelper::MESSAGE_TABLE_AND_QUERY_CANNOT_BE_SET_TOGETHER)
            ->end();

        $node->validate()
            ->ifTrue(function ($v) {
                foreach ($v as $table) {
                    return isset($table['query']) && $table['incremental'];
                }
            })
            ->thenInvalid(ConfigDefinitionValidationHelper::MESSAGE_CUSTOM_QUERY_CANNOT_BE_FETCHED_INCREMENTALLY)
            ->end();
        // @formatter:on

        return $node;
    }
}
