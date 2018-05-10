package kr.jm.utils.elasticsearch;

import kr.jm.utils.datastructure.JMArrays;
import lombok.Getter;
import lombok.Setter;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.util.function.Consumer;

import static java.util.Arrays.stream;
import static kr.jm.utils.elasticsearch.JMElasticsearchUtil.logRequestQueryAndReturn;
import static kr.jm.utils.helper.JMOptional.ifNotNull;
import static kr.jm.utils.helper.JMPredicate.getIsNotNull;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

/**
 * The type Jm elasticsearch search and count.
 */
public class JMElasticsearchSearchAndCount {

    private static final int DefaultHitsCount = 10;
    @Getter
    @Setter
    private static long timeoutMillis = 5000;
    private Client esClient;
    @Getter
    @Setter
    private int defaultHitsCount;

    /**
     * Instantiates a new Jm elasticsearch search and count.
     *
     * @param elasticsearchClient the elasticsearch client
     */
    public JMElasticsearchSearchAndCount(Client elasticsearchClient) {
        this.esClient = elasticsearchClient;
        this.defaultHitsCount = DefaultHitsCount;
    }

    /**
     * Search with target count search response.
     *
     * @param searchRequestBuilder the search request builder
     * @return the search response
     */
    public SearchResponse searchWithTargetCount(
            SearchRequestBuilder searchRequestBuilder) {
        return searchQuery(
                getSearchRequestBuilderWithCount(searchRequestBuilder));
    }

    /**
     * Search with target count search response.
     *
     * @param searchRequestBuilder the search request builder
     * @param aggregationBuilders  the aggregation builders
     * @return the search response
     */
    public SearchResponse searchWithTargetCount(
            SearchRequestBuilder searchRequestBuilder,
            AggregationBuilder[] aggregationBuilders) {
        return searchWithTargetCount(getSearchRequestBuilder(
                getSearchRequestBuilderWithCount(searchRequestBuilder),
                aggregationBuilders));
    }

    /**
     * Search with target count search response.
     *
     * @param isSetExplain                 the is set explain
     * @param indices                      the indices
     * @param types                        the types
     * @param mustConditionQueryBuilders   the must condition query builders
     * @param filterConditionQueryBuilders the filter condition query builders
     * @return the search response
     */
    public SearchResponse searchWithTargetCount(boolean isSetExplain,
            String[] indices, String[] types, QueryBuilder[]
            mustConditionQueryBuilders, QueryBuilder[]
            filterConditionQueryBuilders) {
        return searchWithTargetCount(isSetExplain, indices, types,
                mustConditionQueryBuilders,
                filterConditionQueryBuilders, null);
    }

    /**
     * Search with target count search response.
     *
     * @param isSetExplain                 the is set explain
     * @param indices                      the indices
     * @param types                        the types
     * @param mustConditionQueryBuilders   the must condition query builders
     * @param filterConditionQueryBuilders the filter condition query builders
     * @param aggregationBuilders          the aggregation builders
     * @return the search response
     */
    public SearchResponse searchWithTargetCount(boolean isSetExplain,
            String[] indices, String[] types, QueryBuilder[]
            mustConditionQueryBuilders, QueryBuilder[]
            filterConditionQueryBuilders,
            AggregationBuilder[] aggregationBuilders) {
        return searchWithTargetCount(
                getSearchRequestBuilder(isSetExplain, indices, types,
                        mustConditionQueryBuilders,
                        filterConditionQueryBuilders, null),
                aggregationBuilders);
    }


    /**
     * Gets search request builder.
     *
     * @param queryBuilder the query builder
     * @param indices      the indices
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(QueryBuilder
            queryBuilder, String... indices) {
        return getSearchRequestBuilder(indices, null, queryBuilder);
    }

    /**
     * Gets search request builder.
     *
     * @param queryBuilder        the query builder
     * @param aggregationBuilders the aggregation builders
     * @param indices             the indices
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(
            QueryBuilder queryBuilder,
            AggregationBuilder[] aggregationBuilders, String... indices) {
        return getSearchRequestBuilder(indices, null, queryBuilder,
                aggregationBuilders);
    }

    /**
     * Gets search request builder.
     *
     * @param indices      the indices
     * @param types        the types
     * @param queryBuilder the query builder
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(String[] indices,
            String[] types, QueryBuilder queryBuilder) {
        return getSearchRequestBuilder(indices, types,
                queryBuilder, null);
    }

    /**
     * Gets search request builder.
     *
     * @param indices             the indices
     * @param types               the types
     * @param queryBuilder        the query builder
     * @param aggregationBuilders the aggregation builders
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(String[] indices,
            String[] types, QueryBuilder queryBuilder,
            AggregationBuilder[] aggregationBuilders) {
        return getSearchRequestBuilder(false, indices, types,
                queryBuilder, aggregationBuilders);
    }

    /**
     * Gets search request builder.
     *
     * @param searchRequestBuilder the search request builder
     * @param aggregationBuilders  the aggregation builders
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(
            SearchRequestBuilder searchRequestBuilder,
            AggregationBuilder[] aggregationBuilders) {
        ifNotNull(aggregationBuilders,
                array -> stream(array).filter(getIsNotNull())
                        .forEach(searchRequestBuilder::addAggregation));
        return searchRequestBuilder;
    }

    private SearchRequestBuilder getSearchRequestBuilderWithCount(
            SearchRequestBuilder searchRequestBuilder) {
        return searchRequestBuilder.setSize(
                Long.valueOf(countQuery(searchRequestBuilder)).intValue());
    }


    /**
     * Gets search request builder.
     *
     * @param isSetExplain                 the is set explain
     * @param indices                      the indices
     * @param types                        the types
     * @param mustConditionQueryBuilders   the must condition query builders
     * @param filterConditionQueryBuilders the filter condition query builders
     * @param aggregationBuilders          the aggregation builders
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
            String[] indices, String[] types,
            QueryBuilder[] mustConditionQueryBuilders,
            QueryBuilder[] filterConditionQueryBuilders,
            AggregationBuilder[] aggregationBuilders) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        ifNotNull(mustConditionQueryBuilders,
                array -> buildQueryBuilder(array, boolQueryBuilder::must));
        ifNotNull(filterConditionQueryBuilders,
                array -> buildQueryBuilder(array, boolQueryBuilder::filter));
        return getSearchRequestBuilder(isSetExplain, indices, types,
                boolQueryBuilder, aggregationBuilders);
    }

    private void buildQueryBuilder(QueryBuilder[] array,
            Consumer<QueryBuilder> builderConsumer) {
        stream(array).filter(getIsNotNull()).forEach(builderConsumer);
    }

    /**
     * Gets search request builder.
     *
     * @param isSetExplain the is set explain
     * @param indices      the indices
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
            String[] indices) {
        return getSearchRequestBuilder(isSetExplain, indices, null);
    }

    /**
     * Gets search request builder.
     *
     * @param isSetExplain the is set explain
     * @param indices      the indices
     * @param types        the types
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
            String[] indices, String[] types) {
        return getSearchRequestBuilder(isSetExplain, indices, types, null);
    }

    /**
     * Gets search request builder.
     *
     * @param isSetExplain the is set explain
     * @param indices      the indices
     * @param types        the types
     * @param queryBuilder the query builder
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
            String[] indices, String[] types, QueryBuilder queryBuilder) {
        return getSearchRequestBuilder(isSetExplain, indices, types,
                queryBuilder, null);
    }


    /**
     * Gets search request builder.
     *
     * @param isSetExplain the is set explain
     * @param queryBuilder the query builder
     * @param indices      the indices
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
            QueryBuilder queryBuilder, String... indices) {
        return getSearchRequestBuilder(isSetExplain, indices, null,
                queryBuilder, null);
    }


    /**
     * Gets search request builder.
     *
     * @param isSetExplain        the is set explain
     * @param indices             the indices
     * @param types               the types
     * @param queryBuilder        the query builder
     * @param aggregationBuilders the aggregation builders
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
            String[] indices, String[] types, QueryBuilder queryBuilder,
            AggregationBuilder[] aggregationBuilders) {
        SearchRequestBuilder searchRequestBuilder = getSearchRequestBuilder(
                esClient.prepareSearch(indices)
                        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                        .setSize(defaultHitsCount).setExplain(isSetExplain),
                aggregationBuilders);
        ifNotNull(types, searchRequestBuilder::setTypes);
        ifNotNull(queryBuilder, searchRequestBuilder::setQuery);
        return searchRequestBuilder;
    }


    /**
     * Gets search request builder with match all.
     *
     * @param isSetExplain        the is set explain
     * @param indices             the indices
     * @param types               the types
     * @param filterQueryBuilder  the filter query builder
     * @param aggregationBuilders the aggregation builders
     * @return the search request builder with match all
     */
    public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
            boolean isSetExplain, String[] indices, String[] types,
            QueryBuilder filterQueryBuilder,
            AggregationBuilder[] aggregationBuilders) {
        return getSearchRequestBuilder(isSetExplain, indices, types,
                JMArrays.buildArray(QueryBuilders.matchAllQuery()),
                JMArrays.buildArray(filterQueryBuilder), aggregationBuilders);
    }

    /**
     * Gets search request builder with match all.
     *
     * @param isSetExplain the is set explain
     * @param indices      the indices
     * @param types        the types
     * @return the search request builder with match all
     */
    public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
            boolean isSetExplain, String[] indices, String[] types) {
        return getSearchRequestBuilderWithMatchAll(isSetExplain, indices, types,
                null, null);
    }

    /**
     * Gets search request builder with match all.
     *
     * @param isSetExplain the is set explain
     * @param indices      the indices
     * @return the search request builder with match all
     */
    public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
            boolean isSetExplain, String[] indices) {
        return getSearchRequestBuilderWithMatchAll(isSetExplain, indices, null);
    }

    /**
     * Gets search request builder with match all.
     *
     * @param isSetExplain       the is set explain
     * @param indices            the indices
     * @param types              the types
     * @param filterQueryBuilder the filter query builder
     * @return the search request builder with match all
     */
    public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
            boolean isSetExplain, String[] indices, String[] types,
            QueryBuilder filterQueryBuilder) {
        return getSearchRequestBuilderWithMatchAll(isSetExplain, indices, types,
                filterQueryBuilder, null);
    }

    /**
     * Gets search request builder with match all.
     *
     * @param isSetExplain       the is set explain
     * @param index              the index
     * @param type               the type
     * @param filterQueryBuilder the filter query builder
     * @return the search request builder with match all
     */
    public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
            boolean isSetExplain, String index, String type,
            QueryBuilder filterQueryBuilder) {
        return getSearchRequestBuilderWithMatchAll(isSetExplain,
                JMArrays.buildArray(index), JMArrays.buildArray(type),
                filterQueryBuilder);
    }

    /**
     * Gets search request builder with match all.
     *
     * @param isSetExplain        the is set explain
     * @param indices             the indices
     * @param types               the types
     * @param aggregationBuilders the aggregation builders
     * @return the search request builder with match all
     */
    public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
            boolean isSetExplain, String[] indices, String[] types,
            AggregationBuilder[] aggregationBuilders) {
        return getSearchRequestBuilderWithMatchAll(isSetExplain, indices, types,
                null, aggregationBuilders);
    }

    /**
     * Search all search response.
     *
     * @param indices             the indices
     * @param types               the types
     * @param aggregationBuilders the aggregation builders
     * @return the search response
     */
    public SearchResponse searchAll(String[] indices, String[] types,
            AggregationBuilder[] aggregationBuilders) {
        return searchAll(indices, types, null, aggregationBuilders);
    }

    /**
     * Search all search response.
     *
     * @param indices the indices
     * @param types   the types
     * @return the search response
     */
    public SearchResponse searchAll(String[] indices, String[] types) {
        return searchAll(indices, types, null, null);
    }

    /**
     * Search all search response.
     *
     * @param indices the indices
     * @param type    the type
     * @return the search response
     */
    public SearchResponse searchAll(String[] indices, String type) {
        return searchAll(indices, JMArrays.buildArray(type));
    }

    /**
     * Search all search response.
     *
     * @param indices the indices
     * @return the search response
     */
    public SearchResponse searchAll(String... indices) {
        return searchAll(false, indices);
    }

    /**
     * Search all search response.
     *
     * @param index               the index
     * @param type                the type
     * @param filterQueryBuilder  the filter query builder
     * @param aggregationBuilders the aggregation builders
     * @return the search response
     */
    public SearchResponse searchAll(String index, String type,
            QueryBuilder filterQueryBuilder,
            AggregationBuilder[] aggregationBuilders) {
        return searchAll(JMArrays.buildArray(index), JMArrays.buildArray(type),
                filterQueryBuilder, aggregationBuilders);
    }

    /**
     * Search all search response.
     *
     * @param index              the index
     * @param type               the type
     * @param filterQueryBuilder the filter query builder
     * @return the search response
     */
    public SearchResponse searchAll(String index, String type,
            QueryBuilder filterQueryBuilder) {
        return searchAll(index, type, filterQueryBuilder, null);
    }

    /**
     * Search all search response.
     *
     * @param index               the index
     * @param type                the type
     * @param aggregationBuilders the aggregation builders
     * @return the search response
     */
    public SearchResponse searchAll(String index, String type,
            AggregationBuilder[] aggregationBuilders) {
        return searchAll(index, type, null, aggregationBuilders);
    }

    /**
     * Search all search response.
     *
     * @param index the index
     * @param type  the type
     * @return the search response
     */
    public SearchResponse searchAll(String index, String type) {
        return searchAll(JMArrays.buildArray(index), type);
    }


    /**
     * Search all search response.
     *
     * @param indices            the indices
     * @param types              the types
     * @param filterQueryBuilder the filter query builder
     * @return the search response
     */
    public SearchResponse searchAll(String[] indices, String[] types,
            QueryBuilder filterQueryBuilder) {
        return searchAll(indices, types, filterQueryBuilder, null);
    }

    /**
     * Search all search response.
     *
     * @param indices             the indices
     * @param types               the types
     * @param filterQueryBuilder  the filter query builder
     * @param aggregationBuilders the aggregation builders
     * @return the search response
     */
    public SearchResponse searchAll(String[] indices, String[] types,
            QueryBuilder filterQueryBuilder,
            AggregationBuilder[] aggregationBuilders) {
        return searchAll(false, indices, types, filterQueryBuilder,
                aggregationBuilders);
    }

    /**
     * Search all search response.
     *
     * @param isSetExplain the is set explain
     * @param indices      the indices
     * @return the search response
     */
    public SearchResponse searchAll(boolean isSetExplain, String[] indices) {
        return searchAll(isSetExplain, indices, null);
    }

    /**
     * Search all search response.
     *
     * @param isSetExplain the is set explain
     * @param indices      the indices
     * @param types        the types
     * @return the search response
     */
    public SearchResponse searchAll(boolean isSetExplain, String[] indices,
            String[] types) {
        return searchAll(isSetExplain, indices, types, null, null);
    }


    /**
     * Search all search response.
     *
     * @param isSetExplain        the is set explain
     * @param indices             the indices
     * @param types               the types
     * @param aggregationBuilders the aggregation builders
     * @return the search response
     */
    public SearchResponse searchAll(boolean isSetExplain, String[] indices,
            String[] types, AggregationBuilder[] aggregationBuilders) {
        return searchAll(isSetExplain, indices, types, null,
                aggregationBuilders);
    }

    /**
     * Search all search response.
     *
     * @param isSetExplain       the is set explain
     * @param indices            the indices
     * @param types              the types
     * @param filterQueryBuilder the filter query builder
     * @return the search response
     */
    public SearchResponse searchAll(boolean isSetExplain, String[] indices,
            String[] types, QueryBuilder filterQueryBuilder) {
        return searchAll(isSetExplain, indices, types,
                filterQueryBuilder, null);
    }

    /**
     * Search all search response.
     *
     * @param isSetExplain        the is set explain
     * @param indices             the indices
     * @param types               the types
     * @param filterQueryBuilder  the filter query builder
     * @param aggregationBuilders the aggregation builders
     * @return the search response
     */
    public SearchResponse searchAll(boolean isSetExplain, String[] indices,
            String[] types, QueryBuilder filterQueryBuilder,
            AggregationBuilder[] aggregationBuilders) {
        return searchQuery(getSearchRequestBuilderWithMatchAll(isSetExplain,
                indices, types, filterQueryBuilder,
                aggregationBuilders));
    }

    /**
     * Search all with target count search response.
     *
     * @param indices the indices
     * @param types   the types
     * @return the search response
     */
    public SearchResponse searchAllWithTargetCount(String[] indices,
            String[] types) {
        return searchAllWithTargetCount(indices, types, null, null);
    }

    /**
     * Search all with target count search response.
     *
     * @param indices            the indices
     * @param types              the types
     * @param filterQueryBuilder the filter query builder
     * @return the search response
     */
    public SearchResponse searchAllWithTargetCount(String[] indices,
            String[] types, QueryBuilder filterQueryBuilder) {
        return searchAllWithTargetCount(indices, types, filterQueryBuilder,
                null);
    }

    /**
     * Search all with target count search response.
     *
     * @param indices             the indices
     * @param types               the types
     * @param aggregationBuilders the aggregation builders
     * @return the search response
     */
    public SearchResponse searchAllWithTargetCount(String[] indices,
            String[] types, AggregationBuilder[] aggregationBuilders) {
        return searchAllWithTargetCount(indices, types, null,
                aggregationBuilders);
    }


    /**
     * Search all with target count search response.
     *
     * @param index              the index
     * @param type               the type
     * @param filterQueryBuilder the filter query builder
     * @return the search response
     */
    public SearchResponse searchAllWithTargetCount(String index, String type,
            QueryBuilder filterQueryBuilder) {
        return searchAllWithTargetCount(index, type, filterQueryBuilder, null);
    }

    /**
     * Search all with target count search response.
     *
     * @param index               the index
     * @param type                the type
     * @param aggregationBuilders the aggregation builders
     * @return the search response
     */
    public SearchResponse searchAllWithTargetCount(String index, String type,
            AggregationBuilder[] aggregationBuilders) {
        return searchAllWithTargetCount(index, type, null, aggregationBuilders);
    }

    /**
     * Search all with target count search response.
     *
     * @param index the index
     * @param type  the type
     * @return the search response
     */
    public SearchResponse searchAllWithTargetCount(String index, String type) {
        return searchAllWithTargetCount(index, type, null, null);
    }

    /**
     * Search all with target count search response.
     *
     * @param indices the indices
     * @return the search response
     */
    public SearchResponse searchAllWithTargetCount(String... indices) {
        return searchAllWithTargetCount(indices, null);
    }

    /**
     * Search all with target count search response.
     *
     * @param index               the index
     * @param type                the type
     * @param filterQueryBuilder  the filter query builder
     * @param aggregationBuilders the aggregation builders
     * @return the search response
     */
    public SearchResponse searchAllWithTargetCount(String index, String type,
            QueryBuilder filterQueryBuilder,
            AggregationBuilder[] aggregationBuilders) {
        return searchAllWithTargetCount(JMArrays.buildArray(index),
                JMArrays.buildArray(type), filterQueryBuilder,
                aggregationBuilders);
    }

    /**
     * Search all with target count search response.
     *
     * @param indices             the indices
     * @param types               the types
     * @param filterQueryBuilder  the filter query builder
     * @param aggregationBuilders the aggregation builders
     * @return the search response
     */
    public SearchResponse searchAllWithTargetCount(String[] indices,
            String[] types, QueryBuilder filterQueryBuilder,
            AggregationBuilder[] aggregationBuilders) {
        return searchWithTargetCount(
                getSearchRequestBuilderWithMatchAll(false, indices, types,
                        filterQueryBuilder), aggregationBuilders);
    }

    /**
     * Search query search response.
     *
     * @param searchRequestBuilder the search request builder
     * @return the search response
     */
    public SearchResponse
    searchQuery(SearchRequestBuilder searchRequestBuilder) {
        return searchQuery(searchRequestBuilder, timeoutMillis);
    }

    private SearchResponse searchQuery(String method,
            SearchRequestBuilder searchRequestBuilder, long timeoutMillis) {
        searchRequestBuilder.setTimeout(timeValueMillis(timeoutMillis));
        return logRequestQueryAndReturn(method, searchRequestBuilder,
                searchRequestBuilder.execute(), timeoutMillis);
    }

    /**
     * Search query search response.
     *
     * @param searchRequestBuilder the search request builder
     * @param timeoutMillis        the timeout millis
     * @return the search response
     */
    public SearchResponse searchQuery(SearchRequestBuilder searchRequestBuilder,
            long timeoutMillis) {
        return searchQuery("searchQuery", searchRequestBuilder, timeoutMillis);
    }

    /**
     * Count query long.
     *
     * @param countRequestBuilder the count request builder
     * @return the long
     */
    public long countQuery(SearchRequestBuilder countRequestBuilder) {
        return countQuery(countRequestBuilder, timeoutMillis);
    }

    /**
     * Count query long.
     *
     * @param countRequestBuilder the count request builder
     * @param timeoutMillis       the timeout millis
     * @return the long
     */
    public long countQuery(SearchRequestBuilder countRequestBuilder,
            long timeoutMillis) {
        countRequestBuilder.setSize(0);
        return searchQuery("countQuery", countRequestBuilder, timeoutMillis)
                .getHits().getTotalHits();
    }

    /**
     * Count long.
     *
     * @param indices the indices
     * @return the long
     */
    public long count(String... indices) {
        return count(indices, null);
    }

    /**
     * Count long.
     *
     * @param indices the indices
     * @param types   the types
     * @return the long
     */
    public long count(String[] indices, String[] types) {
        return count(indices, types, null);
    }

    /**
     * Count long.
     *
     * @param indices            the indices
     * @param types              the types
     * @param filterQueryBuilder the filter query builder
     * @return the long
     */
    public long count(String[] indices, String[] types,
            QueryBuilder filterQueryBuilder) {
        return countQuery(getSearchRequestBuilderWithMatchAll(false, indices,
                types, filterQueryBuilder));
    }

}
