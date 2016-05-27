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

public class JMElasticsearchSearchAndCount {

	private Client esClient;

	@Getter
	@Setter
	private int defaultHitsCount = Integer.MAX_VALUE;

	public JMElasticsearchSearchAndCount(Client elasticsearchClient) {
		this.esClient = elasticsearchClient;
	}

	public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
			String... indices) {
		return esClient.prepareSearch(indices)
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setSize(defaultHitsCount).setExplain(isSetExplain);
	}

	public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
			QueryBuilder queryBuilder, String... indices) {
		return getSearchRequestBuilder(isSetExplain, indices)
				.setQuery(queryBuilder);
	}

	public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
			String[] indices, String[] types) {
		return getSearchRequestBuilder(isSetExplain, indices).setTypes(types);
	}

	public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
			String[] indices, String[] types, String[] fields) {
		return getSearchRequestBuilder(isSetExplain, indices, types)
				.addFields(fields);
	}

	public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
			String[] indices, String[] types, QueryBuilder queryBuilder) {
		return getSearchRequestBuilder(isSetExplain, indices, types)
				.setQuery(queryBuilder);
	}

	public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
			String[] indices, String[] types, String[] fields,
			QueryBuilder queryBuilder) {
		return getSearchRequestBuilder(isSetExplain, indices, types, fields)
				.setQuery(queryBuilder);
	}

	public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
			String[] indices, String[] types, QueryBuilder queryBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return addAggregationBuilders(getSearchRequestBuilder(isSetExplain,
				indices, types, queryBuilder), aggregationBuilders);
	}

	public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
			String[] indices, String[] types, String[] fields,
			QueryBuilder queryBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return addAggregationBuilders(getSearchRequestBuilder(isSetExplain,
				indices, types, fields, queryBuilder), aggregationBuilders);
	}

	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String... indices) {
		return getSearchRequestBuilder(isSetExplain, indices)
				.setQuery(QueryBuilders.matchAllQuery());
	}

	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String index, String type,
			FilterBuilder filterBuilder) {
		return getSearchRequestBuilderWithMatchAll(isSetExplain, index)
				.setTypes(type).setQuery(QueryBuilders.filteredQuery(
						QueryBuilders.matchAllQuery(), filterBuilder));
	}

	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String[] indices, String[] types) {
		return getSearchRequestBuilderWithMatchAll(isSetExplain, indices)
				.setTypes(types);
	}

	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String[] indices, String[] types,
			String[] fields) {
		return getSearchRequestBuilderWithMatchAll(isSetExplain, indices, types)
				.addFields(fields);
	}

	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String[] indices, String[] types,
			FilterBuilder filterBuilder) {
		return getSearchRequestBuilderWithMatchAll(isSetExplain, indices, types)
				.setQuery(QueryBuilders.filteredQuery(
						QueryBuilders.matchAllQuery(), filterBuilder));
	}

	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String[] indices, String[] types,
			String[] fields, FilterBuilder filterBuilder) {
		return getSearchRequestBuilder(isSetExplain, indices, types, fields)
				.setQuery(QueryBuilders.filteredQuery(
						QueryBuilders.matchAllQuery(), filterBuilder));
	}

	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String[] indices, String[] types,
			AbstractAggregationBuilder... aggregationBuilders) {
		return addAggregationBuilders(getSearchRequestBuilderWithMatchAll(
				isSetExplain, indices, types), aggregationBuilders);
	}

	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String[] indices, String[] types,
			String[] fields,
			AbstractAggregationBuilder... aggregationBuilders) {
		return addAggregationBuilders(getSearchRequestBuilderWithMatchAll(
				isSetExplain, indices, types, fields), aggregationBuilders);
	}

	public SearchRequestBuilder getSearchRequestBuilderWithMatchAll(
			boolean isSetExplain, String[] indices, String[] types,
			FilterBuilder filterBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return addAggregationBuilders(
				getSearchRequestBuilderWithMatchAll(isSetExplain, indices,
						types, filterBuilder),
				aggregationBuilders);
	}

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

	public SearchResponse searchAll(boolean isSetExplain, String... indices) {
		return searchQuery(
				getSearchRequestBuilderWithMatchAll(isSetExplain, indices));
	}

	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(isSetExplain,
				indices, types));
	}

	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, String[] fields) {
		return searchQuery(
				getSearchRequestBuilder(isSetExplain, indices, types, fields));
	}

	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, FilterBuilder filterBuilder) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(isSetExplain,
				indices, types, filterBuilder));
	}

	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, String[] fields, FilterBuilder filterBuilder) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(isSetExplain,
				indices, types, fields, filterBuilder));
	}

	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, AbstractAggregationBuilder... aggregationBuilders) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(isSetExplain,
				indices, types, aggregationBuilders));
	}

	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, String[] fields,
			AbstractAggregationBuilder... aggregationBuilders) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(isSetExplain,
				indices, types, fields, aggregationBuilders));
	}

	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, FilterBuilder filterBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(isSetExplain,
				indices, types, filterBuilder, aggregationBuilders));
	}

	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, String[] fields, FilterBuilder filterBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(isSetExplain,
				indices, types, fields, filterBuilder, aggregationBuilders));
	}

	public SearchResponse searchAll(String... indices) {
		return searchAll(false, indices);
	}

	public SearchResponse searchAll(String index, String type) {
		return searchQuery(
				getSearchRequestBuilder(false, index).setTypes(type));
	}

	public SearchResponse searchAll(String index, String type,
			FilterBuilder filterBuilder) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(false, index,
				type, filterBuilder));
	}

	public SearchResponse searchAll(String[] indices, String type) {
		return searchQuery(
				getSearchRequestBuilder(false, indices).setTypes(type));
	}

	public SearchResponse searchAll(String[] indices, String[] types) {
		return searchAll(false, indices, types);
	}

	public SearchResponse searchAll(String[] indices, String[] types,
			String[] fields) {
		return searchAll(false, indices, types, fields);
	}

	public SearchResponse searchAll(String[] indices, String[] types,
			FilterBuilder filterBuilder) {
		return searchAll(false, indices, types, filterBuilder);
	}

	public SearchResponse searchAll(String[] indices, String[] types,
			String[] fields, FilterBuilder filterBuilder) {
		return searchAll(false, indices, types, fields, filterBuilder);
	}

	public SearchResponse searchAll(String[] indices, String[] types,
			AbstractAggregationBuilder... aggregationBuilders) {
		return searchAll(false, indices, types, aggregationBuilders);
	}

	public SearchResponse searchAll(String[] indices, String[] types,
			String[] fields,
			AbstractAggregationBuilder... aggregationBuilders) {
		return searchAll(false, indices, types, fields, aggregationBuilders);
	}

	public SearchResponse searchAll(String[] indices, String[] types,
			FilterBuilder filterBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return searchAll(false, indices, types, filterBuilder,
				aggregationBuilders);
	}

	public SearchResponse searchAll(String[] indices, String[] types,
			String[] fields, FilterBuilder filterBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return searchAll(false, indices, types, fields, filterBuilder,
				aggregationBuilders);
	}

	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types) {
		return searchQuery(
				getSearchRequestBuilderWithMatchAll(false, indices, types)
						.setSize(((Long) count(indices, types)).intValue()));
	}

	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, String[] fields) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(false, indices,
				types, fields)
						.setSize(((Long) count(indices, types)).intValue()));
	}

	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, FilterBuilder filterBuilder) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(false, indices,
				types, filterBuilder)
						.setSize(((Long) count(indices, types)).intValue()));
	}

	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, String[] fields, FilterBuilder filterBuilder) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(false, indices,
				types, fields, filterBuilder)
						.setSize(((Long) count(indices, types)).intValue()));
	}

	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, AbstractAggregationBuilder... aggregationBuilders) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(false, indices,
				types, aggregationBuilders)
						.setSize(((Long) count(indices, types)).intValue()));
	}

	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, String[] fields,
			AbstractAggregationBuilder... aggregationBuilders) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(false, indices,
				types, fields, aggregationBuilders)
						.setSize(((Long) count(indices, types)).intValue()));
	}

	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, FilterBuilder filterBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(false, indices,
				types, filterBuilder, aggregationBuilders)
						.setSize(((Long) count(indices, types)).intValue()));
	}

	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, String[] fields, FilterBuilder filterBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return searchQuery(getSearchRequestBuilderWithMatchAll(false, indices,
				types, fields, filterBuilder, aggregationBuilders)
						.setSize(((Long) count(indices, types)).intValue()));
	}

	public SearchResponse searchAllWithField(String index, String type,
			String... fields) {
		return searchQuery(getSearchRequestBuilder(false, index).setTypes(type)
				.addFields(fields));
	}

	public SearchResponse
			searchQuery(SearchRequestBuilder searchRequestBuilder) {
		return JMElastricsearchUtil.logExcuteAndReturn("searchQuery",
				searchRequestBuilder, searchRequestBuilder.execute());
	}

	public CountResponse countQuery(CountRequestBuilder countRequestBuilder) {
		return JMElastricsearchUtil.logExcuteAndReturn("countQuery",
				countRequestBuilder, countRequestBuilder.execute());
	}

	public long count(String... indices) {
		return countQuery(esClient.prepareCount(indices)
				.setQuery(QueryBuilders.matchAllQuery())).getCount();
	}

	public long count(String[] indices, String[] types) {
		return countQuery(esClient.prepareCount(indices).setTypes(types))
				.getCount();
	}

}
