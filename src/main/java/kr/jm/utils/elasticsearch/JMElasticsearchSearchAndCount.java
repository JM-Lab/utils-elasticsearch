package kr.jm.utils.elasticsearch;

import kr.jm.utils.datastructure.JMArrays;
import lombok.Getter;
import lombok.Setter;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.util.Arrays;

import static kr.jm.utils.helper.JMOptional.ifNotNull;

/**
 * The Class JMElasticsearchSearchAndCount.
 */
public class JMElasticsearchSearchAndCount {

	private static final int DefaultHitsCount = 10;

	private Client esClient;

	@Getter
	@Setter
	private int defaultHitsCount;

	/**
	 * Instantiates a new JM elasticsearch search and count.
	 *
	 * @param elasticsearchClient
	 *            the elasticsearch client
	 */
	public JMElasticsearchSearchAndCount(Client elasticsearchClient) {
		this.esClient = elasticsearchClient;
		this.defaultHitsCount = DefaultHitsCount;
	}

	/**
	 * Gets the search request builder.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param fields
	 *            the fields
	 * @param queryBuilder
	 *            the query builder
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search request builder
	 */
	public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
			String[] indices, String[] types, String[] fields,
			QueryBuilder queryBuilder,
			AggregationBuilder[] aggregationBuilders) {
		SearchRequestBuilder searchRequestBuilder =
				esClient.prepareSearch(indices)
						.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
						.setSize(defaultHitsCount).setExplain(isSetExplain);
		ifNotNull(types, searchRequestBuilder::setTypes);
		ifNotNull(fields, searchRequestBuilder::storedFields);
		ifNotNull(queryBuilder, searchRequestBuilder::setQuery);
		ifNotNull(aggregationBuilders, array -> Arrays.stream(array)
				.forEach(searchRequestBuilder::addAggregation));
		return searchRequestBuilder;
	}

	/**
	 * Gets the search request builder.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param fields
	 *            the fields
	 * @param queryBuilder
	 *            the query builder
	 * @return the search request builder
	 */
	public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
			String[] indices, String[] types, String[] fields,
			QueryBuilder queryBuilder) {
		return getSearchRequestBuilder(isSetExplain, indices, types, fields,
				queryBuilder, null);
	}

	/**
	 * Gets the search request builder.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param fields
	 *            the fields
	 * @return the search request builder
	 */
	public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
			String[] indices, String[] types, String[] fields) {
		return getSearchRequestBuilder(isSetExplain, indices, types, fields,
				null);
	}

	/**
	 * Gets the search request builder.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @return the search request builder
	 */
	public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
			String[] indices, String[] types) {
		return getSearchRequestBuilder(isSetExplain, indices, types, null, null,
				null);
	}

	/**
	 * Gets the search request builder.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @return the search request builder
	 */
	public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
			String[] indices) {
		return getSearchRequestBuilder(isSetExplain, indices, null);
	}

	/**
	 * Gets the search request builder.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param queryBuilder
	 *            the query builder
	 * @return the search request builder
	 */
	public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
			String[] indices, String[] types, QueryBuilder queryBuilder) {
		return getSearchRequestBuilder(isSetExplain, indices, types, null,
				queryBuilder);
	}

	/**
	 * Gets the search request builder.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param queryBuilder
	 *            the query builder
	 * @param indices
	 *            the indices
	 * @return the search request builder
	 */
	public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
			QueryBuilder queryBuilder, String... indices) {
		return getSearchRequestBuilder(isSetExplain, indices, null, null,
				queryBuilder);
	}

	/**
	 * Gets the search request builder.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param queryBuilder
	 *            the query builder
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search request builder
	 */
	public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
			String[] indices, String[] types, QueryBuilder queryBuilder,
			AggregationBuilder[] aggregationBuilders) {
		return getSearchRequestBuilder(isSetExplain, indices, types, null,
				queryBuilder, aggregationBuilders);
	}

	/**
	 * Gets the search request builder with match all.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param fields
	 *            the fields
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search request builder with match all
	 */
	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String[] indices, String[] types,
			String[] fields, QueryBuilder filterQueryBuilder,
			AggregationBuilder[] aggregationBuilders) {
		QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
		if (filterQueryBuilder != null)
			queryBuilder = QueryBuilders.boolQuery().must(queryBuilder)
					.filter(filterQueryBuilder);
		return getSearchRequestBuilder(isSetExplain, indices, types, fields,
				queryBuilder, aggregationBuilders);
	}

	/**
	 * Gets the search request builder with match all.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param fields
	 *            the fields
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @return the search request builder with match all
	 */
	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String[] indices, String[] types,
			String[] fields, QueryBuilder filterQueryBuilder) {
		return getSearchRequestBuilderWithMatchAll(isSetExplain, indices, types,
				fields, filterQueryBuilder, null);
	}

	/**
	 * Gets the search request builder with match all.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param fields
	 *            the fields
	 * @return the search request builder with match all
	 */
	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String[] indices, String[] types,
			String[] fields) {
		return getSearchRequestBuilderWithMatchAll(isSetExplain, indices, types,
				fields, null, null);
	}

	/**
	 * Gets the search request builder with match all.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @return the search request builder with match all
	 */
	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String[] indices, String[] types) {
		return getSearchRequestBuilderWithMatchAll(isSetExplain, indices, types,
				null, null, null);
	}

	/**
	 * Gets the search request builder with match all.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @return the search request builder with match all
	 */
	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String[] indices) {
		return getSearchRequestBuilderWithMatchAll(isSetExplain, indices, null);
	}

	/**
	 * Gets the search request builder with match all.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @return the search request builder with match all
	 */
	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String[] indices, String[] types,
			QueryBuilder filterQueryBuilder) {
		return getSearchRequestBuilderWithMatchAll(isSetExplain, indices, types,
				null, filterQueryBuilder, null);
	}

	/**
	 * Gets the search request builder with match all.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param filterQueryBuilder
	 *            the filter query builder
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
	 * Gets the search request builder with match all.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param fields
	 *            the fields
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search request builder with match all
	 */
	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String[] indices, String[] types,
			String[] fields, AggregationBuilder[] aggregationBuilders) {
		return getSearchRequestBuilderWithMatchAll(isSetExplain, indices, types,
				fields, null, aggregationBuilders);
	}

	/**
	 * Search all.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param fields
	 *            the fields
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, String[] fields, QueryBuilder filterQueryBuilder,
			AggregationBuilder[] aggregationBuilders) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(isSetExplain,
				indices, types, fields, filterQueryBuilder,
				aggregationBuilders));
	}

	/**
	 * Search all.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param fields
	 *            the fields
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @return the search response
	 */
	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, String[] fields, QueryBuilder filterQueryBuilder) {
		return searchAll(isSetExplain, indices, types, fields,
				filterQueryBuilder, null);
	}

	/**
	 * Search all.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param fields
	 *            the fields
	 * @return the search response
	 */
	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, String[] fields) {
		return searchAll(isSetExplain, indices, types, fields, null, null);
	}

	/**
	 * Search all.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @return the search response
	 */
	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types) {
		return searchAll(isSetExplain, indices, types, null, null, null);
	}

	/**
	 * Search all.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @return the search response
	 */
	public SearchResponse searchAll(boolean isSetExplain, String[] indices) {
		return searchAll(isSetExplain, indices, null);
	}

	/**
	 * Search all.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @return the search response
	 */
	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, QueryBuilder filterQueryBuilder) {
		return searchAll(isSetExplain, indices, types, null,
				filterQueryBuilder);
	}

	/**
	 * Search all.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param fields
	 *            the fields
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, String[] fields,
			AggregationBuilder[] aggregationBuilders) {
		return searchAll(isSetExplain, indices, types, fields, null,
				aggregationBuilders);

	}

	/**
	 * Search all.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param fields
	 *            the fields
	 * @return the search response
	 */
	public SearchResponse searchAll(String[] indices, String[] types,
			String[] fields) {
		return searchAll(false, indices, types, fields);
	}

	/**
	 * Search all with field.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param fields
	 *            the fields
	 * @return the search response
	 */
	public SearchResponse searchAllWithField(String index, String type,
			String... fields) {
		return searchAll(JMArrays.buildArray(index), JMArrays.buildArray(type),
				fields);
	}

	/**
	 * Search all.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, AggregationBuilder[] aggregationBuilders) {
		return searchAll(isSetExplain, indices, types, null, null,
				aggregationBuilders);
	}

	/**
	 * Search all.
	 *
	 * @param isSetExplain
	 *            the is set explain
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, QueryBuilder filterQueryBuilder,
			AggregationBuilder[] aggregationBuilders) {
		return searchAll(isSetExplain, indices, types, null, filterQueryBuilder,
				aggregationBuilders);
	}

	/**
	 * Search all.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param fields
	 *            the fields
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAll(String[] indices, String[] types,
			String[] fields, QueryBuilder filterQueryBuilder,
			AggregationBuilder[] aggregationBuilders) {
		return searchAll(false, indices, types, fields, filterQueryBuilder,
				aggregationBuilders);
	}

	/**
	 * Search all.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAll(String[] indices, String[] types,
			QueryBuilder filterQueryBuilder,
			AggregationBuilder[] aggregationBuilders) {
		return searchAll(indices, types, null, filterQueryBuilder,
				aggregationBuilders);
	}

	/**
	 * Search all.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @return the search response
	 */
	public SearchResponse searchAll(String[] indices, String[] types,
			QueryBuilder filterQueryBuilder) {
		return searchAll(indices, types, filterQueryBuilder, null);
	}

	/**
	 * Search all.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAll(String[] indices, String[] types,
			AggregationBuilder[] aggregationBuilders) {
		return searchAll(indices, types, null, aggregationBuilders);
	}

	/**
	 * Search all.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @return the search response
	 */
	public SearchResponse searchAll(String[] indices, String[] types) {
		return searchAll(indices, types, null, null);
	}

	/**
	 * Search all.
	 *
	 * @param indices
	 *            the indices
	 * @param type
	 *            the type
	 * @return the search response
	 */
	public SearchResponse searchAll(String[] indices, String type) {
		return searchAll(indices, JMArrays.buildArray(type));
	}

	/**
	 * Search all.
	 *
	 * @param indices
	 *            the indices
	 * @return the search response
	 */
	public SearchResponse searchAll(String... indices) {
		return searchAll(false, indices);
	}

	/**
	 * Search all.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAll(String index, String type,
			QueryBuilder filterQueryBuilder,
			AggregationBuilder[] aggregationBuilders) {
		return searchAll(JMArrays.buildArray(index), JMArrays.buildArray(type),
				filterQueryBuilder, aggregationBuilders);
	}

	/**
	 * Search all.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @return the search response
	 */
	public SearchResponse searchAll(String index, String type,
			QueryBuilder filterQueryBuilder) {
		return searchAll(index, type, filterQueryBuilder, null);
	}

	/**
	 * Search all.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAll(String index, String type,
			AggregationBuilder[] aggregationBuilders) {
		return searchAll(index, type, null, aggregationBuilders);
	}

	/**
	 * Search all.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @return the search response
	 */
	public SearchResponse searchAll(String index, String type) {
		return searchAll(JMArrays.buildArray(index), type);
	}

	/**
	 * Search all with target count.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param fields
	 *            the fields
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, String[] fields, QueryBuilder filterQueryBuilder,
			AggregationBuilder[] aggregationBuilders) {
		return searchQuery(getSearchRequestBuilderWithCount(
				getSearchRequestBuilderWithMatchAll(false, indices, types,
						fields, filterQueryBuilder, aggregationBuilders),
				filterQueryBuilder, indices, types));
	}

	private SearchRequestBuilder getSearchRequestBuilderWithCount(
			SearchRequestBuilder searchRequestBuilder,
			QueryBuilder filterQueryBuilder, String[] indices, String[] types) {
		return searchRequestBuilder.setSize(Long
				.valueOf(count(indices, types, filterQueryBuilder)).intValue());
	}

	/**
	 * Search all with target count.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param fields
	 *            the fields
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @return the search response
	 */
	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, String[] fields, QueryBuilder filterQueryBuilder) {
		return searchAllWithTargetCount(indices, types, fields,
				filterQueryBuilder, null);
	}

	/**
	 * Search all with target count.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param fields
	 *            the fields
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, String[] fields,
			AggregationBuilder[] aggregationBuilders) {
		return searchAllWithTargetCount(indices, types, fields, null,
				aggregationBuilders);
	}

	/**
	 * Search all with target count.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param fields
	 *            the fields
	 * @return the search response
	 */
	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, String[] fields) {
		return searchAllWithTargetCount(indices, types, fields, null, null);
	}

	/**
	 * Search all with target count.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @return the search response
	 */
	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types) {
		return searchAllWithTargetCount(indices, types, null, null, null);
	}

	/**
	 * Search all with target count.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, QueryBuilder filterQueryBuilder,
			AggregationBuilder[] aggregationBuilders) {
		return searchAllWithTargetCount(indices, types, null,
				filterQueryBuilder, aggregationBuilders);
	}

	/**
	 * Search all with target count.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @return the search response
	 */
	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, QueryBuilder filterQueryBuilder) {
		return searchAllWithTargetCount(indices, types, filterQueryBuilder,
				null);
	}

	/**
	 * Search all with target count.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, AggregationBuilder[] aggregationBuilders) {
		return searchAllWithTargetCount(indices, types, null, null,
				aggregationBuilders);
	}

	/**
	 * Search all with target count.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @param aggregationBuilders
	 *            the aggregation builders
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
	 * Search all with target count.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @return the search response
	 */
	public SearchResponse searchAllWithTargetCount(String index, String type,
			QueryBuilder filterQueryBuilder) {
		return searchAllWithTargetCount(index, type, filterQueryBuilder, null);
	}

	/**
	 * Search all with target count.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAllWithTargetCount(String index, String type,
			AggregationBuilder[] aggregationBuilders) {
		return searchAllWithTargetCount(index, type, null, aggregationBuilders);
	}

	/**
	 * Search all with target count.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @return the search response
	 */
	public SearchResponse searchAllWithTargetCount(String index, String type) {
		return searchAllWithTargetCount(index, type, null, null);
	}

	/**
	 * Search all with target count.
	 *
	 * @param indices
	 *            the indices
	 * @return the search response
	 */
	public SearchResponse searchAllWithTargetCount(String... indices) {
		return searchAllWithTargetCount(indices, null);
	}

	/**
	 * Search query.
	 *
	 * @param searchRequestBuilder
	 *            the search request builder
	 * @return the search response
	 */
	public SearchResponse
			searchQuery(SearchRequestBuilder searchRequestBuilder) {
		return JMElastricsearchUtil.logExcuteAndReturn("searchQuery",
				searchRequestBuilder, searchRequestBuilder.execute());
	}

	/**
	 * Count query.
	 *
	 * @param countRequestBuilder
	 *            the count request builder
	 * @return the long
	 */
	public long countQuery(SearchRequestBuilder countRequestBuilder) {
		countRequestBuilder.setSize(0);
		return JMElastricsearchUtil.logExcuteAndReturn("countQuery",
				countRequestBuilder, countRequestBuilder.execute()).getHits()
				.totalHits();
	}

	/**
	 * Count.
	 *
	 * @param indices
	 *            the indices
	 * @return the long
	 */
	public long count(String... indices) {
		return count(indices, null);
	}

	/**
	 * Count.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @return the long
	 */
	public long count(String[] indices, String[] types) {
		return count(indices, types, null);
	}

	public long count(String[] indices, String[] types,
			QueryBuilder filterQueryBuilder) {
		return countQuery(getSearchRequestBuilderWithMatchAll(false, indices,
				types, null, filterQueryBuilder));
	}

}
