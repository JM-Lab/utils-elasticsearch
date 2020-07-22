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
    private final Client esClient;
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
    public SearchResponse searchWithTargetCount(SearchRequestBuilder searchRequestBuilder) {
        return searchQuery(getSearchRequestBuilderWithCount(searchRequestBuilder));
    }

    /**
     * Search with target count search response.
     *
     * @param searchRequestBuilder the search request builder
     * @param aggregationBuilders  the aggregation builders
     * @return the search response
     */
    public SearchResponse searchWithTargetCount(SearchRequestBuilder searchRequestBuilder,
            AggregationBuilder[] aggregationBuilders) {
        return searchWithTargetCount(
                getSearchRequestBuilder(getSearchRequestBuilderWithCount(searchRequestBuilder), aggregationBuilders));
    }

    /**
     * Search with target count search response.
     *
     * @param isSetExplain                 the is set explain
     * @param indices                      the indices
     * @param mustConditionQueryBuilders   the must condition query builders
     * @param filterConditionQueryBuilders the filter condition query builders
     * @return the search response
     */
    public SearchResponse searchWithTargetCount(boolean isSetExplain, String[] indices,
            QueryBuilder[] mustConditionQueryBuilders, QueryBuilder[] filterConditionQueryBuilders) {
        return searchWithTargetCount(isSetExplain, indices, mustConditionQueryBuilders,
                filterConditionQueryBuilders, null);
    }

    /**
     * Search with target count search response.
     *
     * @param isSetExplain                 the is set explain
     * @param indices                      the indices
     * @param mustConditionQueryBuilders   the must condition query builders
     * @param filterConditionQueryBuilders the filter condition query builders
     * @param aggregationBuilders          the aggregation builders
     * @return the search response
     */
    public SearchResponse searchWithTargetCount(boolean isSetExplain, String[] indices,
            QueryBuilder[] mustConditionQueryBuilders, QueryBuilder[] filterConditionQueryBuilders,
            AggregationBuilder[] aggregationBuilders) {
        return searchWithTargetCount(getSearchRequestBuilder(isSetExplain, indices, mustConditionQueryBuilders,
                filterConditionQueryBuilders, null), aggregationBuilders);
    }


    /**
     * Gets search request builder.
     *
     * @param queryBuilder the query builder
     * @param indices      the indices
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(QueryBuilder queryBuilder, String... indices) {
        return getSearchRequestBuilder(indices, queryBuilder);
    }

    /**
     * Gets search request builder.
     *
     * @param queryBuilder        the query builder
     * @param aggregationBuilders the aggregation builders
     * @param indices             the indices
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(QueryBuilder queryBuilder,
            AggregationBuilder[] aggregationBuilders, String... indices) {
        return getSearchRequestBuilder(indices, queryBuilder, aggregationBuilders);
    }

    /**
     * Gets search request builder.
     *
     * @param indices      the indices
     * @param queryBuilder the query builder
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(String[] indices, QueryBuilder queryBuilder) {
        return getSearchRequestBuilder(indices, queryBuilder, null);
    }

    /**
     * Gets search request builder.
     *
     * @param indices             the indices
     * @param queryBuilder        the query builder
     * @param aggregationBuilders the aggregation builders
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(String[] indices, QueryBuilder queryBuilder,
            AggregationBuilder[] aggregationBuilders) {
        return getSearchRequestBuilder(false, indices, queryBuilder, aggregationBuilders);
    }

    /**
     * Gets search request builder.
     *
     * @param searchRequestBuilder the search request builder
     * @param aggregationBuilders  the aggregation builders
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(SearchRequestBuilder searchRequestBuilder,
            AggregationBuilder[] aggregationBuilders) {
        ifNotNull(aggregationBuilders,
                array -> stream(array).filter(getIsNotNull()).forEach(searchRequestBuilder::addAggregation));
        return searchRequestBuilder;
    }

    private SearchRequestBuilder getSearchRequestBuilderWithCount(SearchRequestBuilder searchRequestBuilder) {
        return searchRequestBuilder.setSize(Long.valueOf(countQuery(searchRequestBuilder)).intValue());
    }


    /**
     * Gets search request builder.
     *
     * @param isSetExplain                 the is set explain
     * @param indices                      the indices
     * @param mustConditionQueryBuilders   the must condition query builders
     * @param filterConditionQueryBuilders the filter condition query builders
     * @param aggregationBuilders          the aggregation builders
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain, String[] indices,
            QueryBuilder[] mustConditionQueryBuilders, QueryBuilder[] filterConditionQueryBuilders,
            AggregationBuilder[] aggregationBuilders) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        ifNotNull(mustConditionQueryBuilders, array -> buildQueryBuilder(array, boolQueryBuilder::must));
        ifNotNull(filterConditionQueryBuilders, array -> buildQueryBuilder(array, boolQueryBuilder::filter));
        return getSearchRequestBuilder(isSetExplain, indices, boolQueryBuilder, aggregationBuilders);
    }

    private void buildQueryBuilder(QueryBuilder[] array, Consumer<QueryBuilder> builderConsumer) {
        stream(array).filter(getIsNotNull()).forEach(builderConsumer);
    }

    /**
     * Gets search request builder.
     *
     * @param isSetExplain the is set explain
     * @param indices      the indices
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain, String[] indices) {
        return getSearchRequestBuilder(isSetExplain, indices, null);
    }

    /**
     * Gets search request builder.
     *
     * @param isSetExplain the is set explain
     * @param indices      the indices
     * @param queryBuilder the query builder
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain, String[] indices,
            QueryBuilder queryBuilder) {
        return getSearchRequestBuilder(isSetExplain, indices, queryBuilder, null);
    }


    /**
     * Gets search request builder.
     *
     * @param isSetExplain the is set explain
     * @param queryBuilder the query builder
     * @param indices      the indices
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain, QueryBuilder queryBuilder,
            String... indices) {
        return getSearchRequestBuilder(isSetExplain, indices, queryBuilder, null);
    }


    /**
     * Gets search request builder.
     *
     * @param isSetExplain        the is set explain
     * @param indices             the indices
     * @param queryBuilder        the query builder
     * @param aggregationBuilders the aggregation builders
     * @return the search request builder
     */
    public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain, String[] indices,
            QueryBuilder queryBuilder, AggregationBuilder[] aggregationBuilders) {
        SearchRequestBuilder searchRequestBuilder = getSearchRequestBuilder(
                esClient.prepareSearch(indices).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setSize(defaultHitsCount)
                        .setExplain(isSetExplain), aggregationBuilders);
        ifNotNull(queryBuilder, searchRequestBuilder::setQuery);
        return searchRequestBuilder;
    }


    /**
     * Gets search request builder with match all.
     *
     * @param isSetExplain        the is set explain
     * @param indices             the indices
     * @param filterQueryBuilder  the filter query builder
     * @param aggregationBuilders the aggregation builders
     * @return the search request builder with match all
     */
    public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(boolean isSetExplain, String[] indices,
            QueryBuilder filterQueryBuilder, AggregationBuilder[] aggregationBuilders) {
        return getSearchRequestBuilder(isSetExplain, indices, JMArrays.buildArray(QueryBuilders.matchAllQuery()),
                JMArrays.buildArray(filterQueryBuilder), aggregationBuilders);
    }

    /**
     * Gets search request builder with match all.
     *
     * @param isSetExplain the is set explain
     * @param indices      the indices
     * @return the search request builder with match all
     */
    public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(boolean isSetExplain, String[] indices) {
        return getSearchRequestBuilderWithMatchAll(isSetExplain, indices, (QueryBuilder) null);
    }

    /**
     * Gets search request builder with match all.
     *
     * @param isSetExplain       the is set explain
     * @param indices            the indices
     * @param filterQueryBuilder the filter query builder
     * @return the search request builder with match all
     */
    public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(boolean isSetExplain, String[] indices,
            QueryBuilder filterQueryBuilder) {
        return getSearchRequestBuilderWithMatchAll(isSetExplain, indices, filterQueryBuilder, null);
    }

    /**
     * Gets search request builder with match all.
     *
     * @param isSetExplain       the is set explain
     * @param index              the index
     * @param filterQueryBuilder the filter query builder
     * @return the search request builder with match all
     */
    public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(boolean isSetExplain, String index,
            QueryBuilder filterQueryBuilder) {
        return getSearchRequestBuilderWithMatchAll(isSetExplain, JMArrays.buildArray(index), filterQueryBuilder);
    }

    /**
     * Gets search request builder with match all.
     *
     * @param isSetExplain        the is set explain
     * @param indices             the indices
     * @param aggregationBuilders the aggregation builders
     * @return the search request builder with match all
     */
    public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(boolean isSetExplain, String[] indices,
            AggregationBuilder[] aggregationBuilders) {
        return getSearchRequestBuilderWithMatchAll(isSetExplain, indices, null, aggregationBuilders);
    }

    /**
     * Search all search response.
     *
     * @param indices             the indices
     * @param aggregationBuilders the aggregation builders
     * @return the search response
     */
    public SearchResponse searchAll(String[] indices, AggregationBuilder[] aggregationBuilders) {
        return searchAll(indices, null, aggregationBuilders);
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
     * @param filterQueryBuilder  the filter query builder
     * @param aggregationBuilders the aggregation builders
     * @return the search response
     */
    public SearchResponse searchAll(String index, QueryBuilder filterQueryBuilder,
            AggregationBuilder[] aggregationBuilders) {
        return searchAll(JMArrays.buildArray(index), filterQueryBuilder,
                aggregationBuilders);
    }

    /**
     * Search all search response.
     *
     * @param index              the index
     * @param filterQueryBuilder the filter query builder
     * @return the search response
     */
    public SearchResponse searchAll(String index, QueryBuilder filterQueryBuilder) {
        return searchAll(index, filterQueryBuilder, null);
    }

    /**
     * Search all search response.
     *
     * @param index               the index
     * @param aggregationBuilders the aggregation builders
     * @return the search response
     */
    public SearchResponse searchAll(String index, AggregationBuilder[] aggregationBuilders) {
        return searchAll(index, null, aggregationBuilders);
    }

    /**
     * Search all search response.
     *
     * @param indices            the indices
     * @param filterQueryBuilder the filter query builder
     * @return the search response
     */
    public SearchResponse searchAll(String[] indices, QueryBuilder filterQueryBuilder) {
        return searchAll(indices, filterQueryBuilder, null);
    }

    /**
     * Search all search response.
     *
     * @param indices             the indices
     * @param filterQueryBuilder  the filter query builder
     * @param aggregationBuilders the aggregation builders
     * @return the search response
     */
    public SearchResponse searchAll(String[] indices, QueryBuilder filterQueryBuilder,
            AggregationBuilder[] aggregationBuilders) {
        return searchAll(false, indices, filterQueryBuilder, aggregationBuilders);
    }

    /**
     * Search all search response.
     *
     * @param isSetExplain the is set explain
     * @param indices      the indices
     * @return the search response
     */
    public SearchResponse searchAll(boolean isSetExplain, String[] indices) {
        return searchAll(isSetExplain, indices, (QueryBuilder) null);
    }

    /**
     * Search all search response.
     *
     * @param isSetExplain        the is set explain
     * @param indices             the indices
     * @param aggregationBuilders the aggregation builders
     * @return the search response
     */
    public SearchResponse searchAll(boolean isSetExplain, String[] indices, AggregationBuilder[] aggregationBuilders) {
        return searchAll(isSetExplain, indices, null, aggregationBuilders);
    }

    /**
     * Search all search response.
     *
     * @param isSetExplain       the is set explain
     * @param indices            the indices
     * @param filterQueryBuilder the filter query builder
     * @return the search response
     */
    public SearchResponse searchAll(boolean isSetExplain, String[] indices, QueryBuilder filterQueryBuilder) {
        return searchAll(isSetExplain, indices, filterQueryBuilder, null);
    }

    /**
     * Search all search response.
     *
     * @param isSetExplain        the is set explain
     * @param indices             the indices
     * @param filterQueryBuilder  the filter query builder
     * @param aggregationBuilders the aggregation builders
     * @return the search response
     */
    public SearchResponse searchAll(boolean isSetExplain, String[] indices, QueryBuilder filterQueryBuilder,
            AggregationBuilder[] aggregationBuilders) {
        return searchQuery(getSearchRequestBuilderWithMatchAll(isSetExplain, indices, filterQueryBuilder,
                aggregationBuilders));
    }

    /**
     * Search all with target count search response.
     *
     * @param indices            the indices
     * @param filterQueryBuilder the filter query builder
     * @return the search response
     */
    public SearchResponse searchAllWithTargetCount(String[] indices, QueryBuilder filterQueryBuilder) {
        return searchAllWithTargetCount(indices, filterQueryBuilder, null);
    }

    /**
     * Search all with target count search response.
     *
     * @param indices             the indices
     * @param aggregationBuilders the aggregation builders
     * @return the search response
     */
    public SearchResponse searchAllWithTargetCount(String[] indices, AggregationBuilder[] aggregationBuilders) {
        return searchAllWithTargetCount(indices, null, aggregationBuilders);
    }


    /**
     * Search all with target count search response.
     *
     * @param index              the index
     * @param filterQueryBuilder the filter query builder
     * @return the search response
     */
    public SearchResponse searchAllWithTargetCount(String index, QueryBuilder filterQueryBuilder) {
        return searchAllWithTargetCount(index, filterQueryBuilder, null);
    }

    /**
     * Search all with target count search response.
     *
     * @param index               the index
     * @param aggregationBuilders the aggregation builders
     * @return the search response
     */
    public SearchResponse searchAllWithTargetCount(String index, AggregationBuilder[] aggregationBuilders) {
        return searchAllWithTargetCount(index, null, aggregationBuilders);
    }

    /**
     * Search all with target count search response.
     *
     * @param indices the indices
     * @return the search response
     */
    public SearchResponse searchAllWithTargetCount(String... indices) {
        return searchAllWithTargetCount(indices, (QueryBuilder) null);
    }

    /**
     * Search all with target count search response.
     *
     * @param index               the index
     * @param filterQueryBuilder  the filter query builder
     * @param aggregationBuilders the aggregation builders
     * @return the search response
     */
    public SearchResponse searchAllWithTargetCount(String index, QueryBuilder filterQueryBuilder,
            AggregationBuilder[] aggregationBuilders) {
        return searchAllWithTargetCount(JMArrays.buildArray(index), filterQueryBuilder, aggregationBuilders);
    }

    /**
     * Search all with target count search response.
     *
     * @param indices             the indices
     * @param filterQueryBuilder  the filter query builder
     * @param aggregationBuilders the aggregation builders
     * @return the search response
     */
    public SearchResponse searchAllWithTargetCount(String[] indices, QueryBuilder filterQueryBuilder,
            AggregationBuilder[] aggregationBuilders) {
        return searchWithTargetCount(getSearchRequestBuilderWithMatchAll(false, indices, filterQueryBuilder),
                aggregationBuilders);
    }

    /**
     * Search query search response.
     *
     * @param searchRequestBuilder the search request builder
     * @return the search response
     */
    public SearchResponse searchQuery(SearchRequestBuilder searchRequestBuilder) {
        return searchQuery(searchRequestBuilder, timeoutMillis);
    }

    private SearchResponse searchQuery(String method, SearchRequestBuilder searchRequestBuilder, long timeoutMillis) {
        searchRequestBuilder.setTimeout(timeValueMillis(timeoutMillis));
        return logRequestQueryAndReturn(method, searchRequestBuilder, searchRequestBuilder.execute(), timeoutMillis);
    }

    /**
     * Search query search response.
     *
     * @param searchRequestBuilder the search request builder
     * @param timeoutMillis        the timeout millis
     * @return the search response
     */
    public SearchResponse searchQuery(SearchRequestBuilder searchRequestBuilder, long timeoutMillis) {
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
    public long countQuery(SearchRequestBuilder countRequestBuilder, long timeoutMillis) {
        countRequestBuilder.setSize(0);
        return searchQuery("countQuery", countRequestBuilder, timeoutMillis).getHits().getTotalHits().value;
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
     * @param indices            the indices
     * @param filterQueryBuilder the filter query builder
     * @return the long
     */
    public long count(String[] indices, QueryBuilder filterQueryBuilder) {
        return countQuery(getSearchRequestBuilderWithMatchAll(false, indices, filterQueryBuilder));
    }

}
