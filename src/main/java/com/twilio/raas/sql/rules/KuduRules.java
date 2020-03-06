package com.twilio.raas.sql.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;

import java.util.Arrays;
import java.util.List;

public class KuduRules {

    public static final KuduFilterRule FILTER = new KuduFilterRule(RelFactories.LOGICAL_BUILDER);
    public static final KuduProjectRule PROJECT = new KuduProjectRule(RelFactories.LOGICAL_BUILDER);
    public static final RelOptRule FILTER_SORT = KuduSortRule.FILTER_SORT_RULE;
    public static final RelOptRule SORT = KuduSortRule.SIMPLE_SORT_RULE;
    public static final KuduLimitRule LIMIT = new KuduLimitRule();
    public static final KuduSortJoinTransposeRule SORT_JOIN_TRANSPOSE =
            new KuduSortJoinTransposeRule(LogicalSort.class, LogicalFilter.class,
                    LogicalJoin.class, RelFactories.LOGICAL_BUILDER);
    public static final KuduNestedJoinRule NESTED_JOIN = new KuduNestedJoinRule(RelFactories.LOGICAL_BUILDER);


    public static List<RelOptRule> RULES = Arrays.asList(
            FILTER,
            PROJECT,
            SORT,
            FILTER_SORT,
            LIMIT,
            SORT_JOIN_TRANSPOSE,
            KuduSortedAggregationRule.SORTED_AGGREGATION_RULE,
            KuduSortedAggregationRule.SORTED_AGGREGATION_LIMIT_RULE,
            NESTED_JOIN
    );
}
