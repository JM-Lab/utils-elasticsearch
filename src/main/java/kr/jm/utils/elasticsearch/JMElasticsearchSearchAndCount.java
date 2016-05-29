package kr.jm.utils.elasticsearch;

import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;

import lombok.Getter;
import lombok.Setter;

/**
 * The Class JMElasticsearchSearchAndCount.
 */
public class JMElasticsearchSearchAndCount {

	private Client esClient;

	/**
	 * Gets the default hits count.
	 *
	 * @return the default hits count
	 */

	/**
	 * Gets the default hits count.
	 *
	 * @return the default hits count
	 */
	@Getter

	/**
	 * Sets the default hits count.
	 *
	 * @param defaultHitsCount
	 *            the new default hits count
	 */

	/**
	 * Sets the default hits count.
	 *
	 * @param defaultHitsCount
	 *            the new default hits count
	 */
	@Setter
	private int defaultHitsCount = Integer.MAX_VALUE;

	/**
	 * Instantiates a new JM elasticsearch search and count.
	 *
	 * @param elasticsearchClient
	 *            the elasticsearch client
	 */
	public JMElasticsearchSearchAndCount(Client elasticsearchClient) {
		this.esClient = elasticsearchClient;
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
			String... indices) {
		return esClient.prepareSearch(indices)
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setSize(defaultHitsCount).setExplain(isSetExplain);
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
		return getSearchRequestBuilder(isSetExplain, indices)
				.setQuery(queryBuilder);
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
		return getSearchRequestBuilder(isSetExplain, indices).setTypes(types);
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
		return getSearchRequestBuilder(isSetExplain, indices, types)
				.addFields(fields);
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
		return getSearchRequestBuilder(isSetExplain, indices, types)
				.setQuery(queryBuilder);
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
		return getSearchRequestBuilder(isSetExplain, indices, types, fields)
				.setQuery(queryBuilder);
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
			AbstractAggregationBuilder... aggregationBuilders) {
		return addAggregationBuilders(getSearchRequestBuilder(isSetExplain,
				indices, types, queryBuilder), aggregationBuilders);
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
			AbstractAggregationBuilder... aggregationBuilders) {
		return addAggregationBuilders(getSearchRequestBuilder(isSetExplain,
				indices, types, fields, queryBuilder), aggregationBuilders);
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
			boolean isSetExplain, String... indices) {
		return getSearchRequestBuilder(isSetExplain, indices)
				.setQuery(QueryBuilders.matchAllQuery());
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
	 * @param filterBuilder
	 *            the filter builder
	 * @return the search request builder with match all
	 */
	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String index, String type,
			FilterBuilder filterBuilder) {
		return getSearchRequestBuilderWithMatchAll(isSetExplain, index)
				.setTypes(type).setQuery(QueryBuilders.filteredQuery(
						QueryBuilders.matchAllQuery(), filterBuilder));
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
		return getSearchRequestBuilderWithMatchAll(isSetExplain, indices)
				.setTypes(types);
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
		return getSearchRequestBuilderWithMatchAll(isSetExplain, indices, types)
				.addFields(fields);
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
	 * @param filterBuilder
	 *            the filter builder
	 * @return the search request builder with match all
	 */
	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String[] indices, String[] types,
			FilterBuilder filterBuilder) {
		return getSearchRequestBuilderWithMatchAll(isSetExplain, indices, types)
				.setQuery(QueryBuilders.filteredQuery(
						QueryBuilders.matchAllQuery(), filterBuilder));
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
	 * @param filterBuilder
	 *            the filter builder
	 * @return the search request builder with match all
	 */
	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String[] indices, String[] types,
			String[] fields, FilterBuilder filterBuilder) {
		return getSearchRequestBuilder(isSetExplain, indices, types, fields)
				.setQuery(QueryBuilders.filteredQuery(
						QueryBuilders.matchAllQuery(), filterBuilder));
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
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search request builder with match all
	 */
	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String[] indices, String[] types,
			AbstractAggregationBuilder... aggregationBuilders) {
		return addAggregationBuilders(getSearchRequestBuilderWithMatchAll(
				isSetExplain, indices, types), aggregationBuilders);
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
			String[] fields,
			AbstractAggregationBuilder... aggregationBuilders) {
		return addAggregationBuilders(getSearchRequestBuilderWithMatchAll(
				isSetExplain, indices, types, fields), aggregationBuilders);
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
	 * @param filterBuilder
	 *            the filter builder
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search request builder with match all
	 */
	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String[] indices, String[] types,
			FilterBuilder filterBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return addAggregationBuilders(
				getSearchRequestBuilderWithMatchAll(isSetExplain, indices,
						types, filterBuilder),
				aggregationBuilders);
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
	 * @param filterBuilder
	 *            the filter builder
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search request builder with match all
	 */
	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String[] indices, String[] types,
			String[] fields, FilterBuilder filterBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return addAggregationBuilders(
				getSearchRequestBuilderWithMatchAll(isSetExplain, indices,
						types, fields, filterBuilder),
				aggregationBuilders);
	}

	private SearchRequestBuilder addAggregationBuilders(
			SearchRequestBuilder searchRequestBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		for (AbstractAggregationBuilder aggregationBuilder : aggregationBuilders)
			searchRequestBuilder.addAggregation(aggregationBuilder);
		return searchRequestBuilder;
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
	public SearchResponse searchAll(boolean isSetExplain, String... indices) {
		return searchQuery(
				getSearchRequestBuilderWithMatchAll(isSetExplain, indices));
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
		return searchQuery(getSearchRequestBuilderWithMatchAll(isSetExplain,
				indices, types));
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
		return searchQuery(
				getSearchRequestBuilder(isSetExplain, indices, types, fields));
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
	 * @param filterBuilder
	 *            the filter builder
	 * @return the search response
	 */
	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, FilterBuilder filterBuilder) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(isSetExplain,
				indices, types, filterBuilder));
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
	 * @param filterBuilder
	 *            the filter builder
	 * @return the search response
	 */
	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, String[] fields, FilterBuilder filterBuilder) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(isSetExplain,
				indices, types, fields, filterBuilder));
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
			String[] types, AbstractAggregationBuilder... aggregationBuilders) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(isSetExplain,
				indices, types, aggregationBuilders));
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
			AbstractAggregationBuilder... aggregationBuilders) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(isSetExplain,
				indices, types, fields, aggregationBuilders));
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
	 * @param filterBuilder
	 *            the filter builder
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, FilterBuilder filterBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(isSetExplain,
				indices, types, filterBuilder, aggregationBuilders));
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
	 * @param filterBuilder
	 *            the filter builder
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, String[] fields, FilterBuilder filterBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(isSetExplain,
				indices, types, fields, filterBuilder, aggregationBuilders));
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
	 * @return the search response
	 */
	public SearchResponse searchAll(String index, String type) {
		return searchQuery(
				getSearchRequestBuilder(false, index).setTypes(type));
	}

	/**
	 * Search all.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param filterBuilder
	 *            the filter builder
	 * @return the search response
	 */
	public SearchResponse searchAll(String index, String type,
			FilterBuilder filterBuilder) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(false, index,
				type, filterBuilder));
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
		return searchQuery(
				getSearchRequestBuilder(false, indices).setTypes(type));
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
		return searchAll(false, indices, types);
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
	 * Search all.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param filterBuilder
	 *            the filter builder
	 * @return the search response
	 */
	public SearchResponse searchAll(String[] indices, String[] types,
			FilterBuilder filterBuilder) {
		return searchAll(false, indices, types, filterBuilder);
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
	 * @param filterBuilder
	 *            the filter builder
	 * @return the search response
	 */
	public SearchResponse searchAll(String[] indices, String[] types,
			String[] fields, FilterBuilder filterBuilder) {
		return searchAll(false, indices, types, fields, filterBuilder);
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
			AbstractAggregationBuilder... aggregationBuilders) {
		return searchAll(false, indices, types, aggregationBuilders);
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
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAll(String[] indices, String[] types,
			String[] fields,
			AbstractAggregationBuilder... aggregationBuilders) {
		return searchAll(false, indices, types, fields, aggregationBuilders);
	}

	/**
	 * Search all.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param filterBuilder
	 *            the filter builder
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAll(String[] indices, String[] types,
			FilterBuilder filterBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return searchAll(false, indices, types, filterBuilder,
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
	 * @param filterBuilder
	 *            the filter builder
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAll(String[] indices, String[] types,
			String[] fields, FilterBuilder filterBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return searchAll(false, indices, types, fields, filterBuilder,
				aggregationBuilders);
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
		return searchQuery(
				getSearchRequestBuilderWithMatchAll(false, indices, types)
						.setSize(((Long) count(indices, types)).intValue()));
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
		return searchQuery(getSearchRequestBuilderWithMatchAll(false, indices,
				types, fields)
						.setSize(((Long) count(indices, types)).intValue()));
	}

	/**
	 * Search all with target count.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param filterBuilder
	 *            the filter builder
	 * @return the search response
	 */
	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, FilterBuilder filterBuilder) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(false, indices,
				types, filterBuilder)
						.setSize(((Long) count(indices, types)).intValue()));
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
	 * @param filterBuilder
	 *            the filter builder
	 * @return the search response
	 */
	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, String[] fields, FilterBuilder filterBuilder) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(false, indices,
				types, fields, filterBuilder)
						.setSize(((Long) count(indices, types)).intValue()));
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
			String[] types, AbstractAggregationBuilder... aggregationBuilders) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(false, indices,
				types, aggregationBuilders)
						.setSize(((Long) count(indices, types)).intValue()));
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
			AbstractAggregationBuilder... aggregationBuilders) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(false, indices,
				types, fields, aggregationBuilders)
						.setSize(((Long) count(indices, types)).intValue()));
	}

	/**
	 * Search all with target count.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param filterBuilder
	 *            the filter builder
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, FilterBuilder filterBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(false, indices,
				types, filterBuilder, aggregationBuilders)
						.setSize(((Long) count(indices, types)).intValue()));
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
	 * @param filterBuilder
	 *            the filter builder
	 * @param aggregationBuilders
	 *            the aggregation builders
	 * @return the search response
	 */
	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, String[] fields, FilterBuilder filterBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(false, indices,
				types, fields, filterBuilder, aggregationBuilders)
						.setSize(((Long) count(indices, types)).intValue()));
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
		return searchQuery(getSearchRequestBuilder(false, index).setTypes(type)
				.addFields(fields));
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
	 * @return the count response
	 */
	public CountResponse countQuery(CountRequestBuilder countRequestBuilder) {
		return JMElastricsearchUtil.logExcuteAndReturn("countQuery",
				countRequestBuilder, countRequestBuilder.execute());
	}

	/**
	 * Count.
	 *
	 * @param indices
	 *            the indices
	 * @return the long
	 */
	public long count(String... indices) {
		return countQuery(esClient.prepareCount(indices)
				.setQuery(QueryBuilders.matchAllQuery())).getCount();
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
		return countQuery(esClient.prepareCount(indices).setTypes(types))
				.getCount();
	}

}
