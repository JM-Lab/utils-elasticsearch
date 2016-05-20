package kr.jm.utils.elasticsearch;

import static java.util.stream.Collectors.toSet;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import kr.jm.utils.datastructure.JMArrays;
import kr.jm.utils.exception.JMExceptionManager;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JMElasticsearchClient implements Client {

	private static final String LOCALHOST_9300 = "localhost:9300";
	private static final String NETWORK_HOST = "network.host";
	private static final String CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME =
			"client.transport.ignore_cluster_name";
	private static final String CLUSTER_NAME = "cluster.name";
	private static final String CLIENT_TRANSPORT_SNIFF =
			"client.transport.sniff";
	private static final ObjectMapper JsonMapper = new ObjectMapper();

	private String ipPortListByComma;
	private boolean isTransportClient;
	@Getter
	private Settings settings;

	@Delegate
	private Client elasticsearchClient;

	@Getter
	@Setter
	private long timeoutMillis = 5000;
	@Getter
	@Setter
	private int retryCount = 2;
	@Getter
	@Setter
	private int defaultHitsCount = 1000;

	public JMElasticsearchClient() {
		this(true, LOCALHOST_9300, getSettingBuilderWithIgnoreClusterName());
	}

	public JMElasticsearchClient(Client elasticsearchClient) {
		this.elasticsearchClient = elasticsearchClient;
	}

	public JMElasticsearchClient(String ipPortListByComma) {
		this(true, ipPortListByComma, getSettingBuilderWithIgnoreClusterName());
	}

	public JMElasticsearchClient(String ipPortListByComma,
			boolean clientTransportSniff) {
		this(true, ipPortListByComma,
				getSettingsWithClientTransportSniff(
						getSettingBuilderWithIgnoreClusterName(),
						clientTransportSniff));
	}

	public JMElasticsearchClient(String ipPortListByComma,
			boolean clientTransportSniff, String clusterName) {
		this(true, ipPortListByComma,
				getSettingsWithClientTransportSniff(
						getSettingBuilderWithClusterName(clusterName),
						clientTransportSniff));
	}

	public JMElasticsearchClient(boolean isTransportClient,
			String ipPortListByComma) {
		this(isTransportClient, ipPortListByComma,
				getSettingBuilderWithIgnoreClusterName());
	}

	public JMElasticsearchClient(boolean isTransportClient,
			String ipPortListByComma, boolean clientTransportSniff) {
		this(true, ipPortListByComma,
				getSettingsWithClientTransportSniff(
						getSettingBuilderWithIgnoreClusterName(),
						clientTransportSniff));
	}

	private static Settings getSettingBuilderWithIgnoreClusterName() {
		return ImmutableSettings.settingsBuilder()
				.put(CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME, true).build();
	}

	public JMElasticsearchClient(boolean isTransportClient,
			String ipPortListByComma, String clusterName) {
		this(isTransportClient, ipPortListByComma,
				getSettingBuilderWithClusterName(clusterName));
	}

	public JMElasticsearchClient(boolean isTransportClient,
			String ipPortListByComma, boolean clientTransportSniff,
			String clusterName) {
		this(isTransportClient, ipPortListByComma,
				getSettingsWithClientTransportSniff(
						getSettingBuilderWithClusterName(clusterName),
						clientTransportSniff));
	}

	public JMElasticsearchClient(boolean isTransportClient,
			String ipPortListByComma, Settings settings) {
		this.isTransportClient = isTransportClient;
		this.ipPortListByComma = ipPortListByComma;
		this.settings = isTransportClient ? settings
				: ImmutableSettings.settingsBuilder()
						.put(NETWORK_HOST, ipPortListByComma).put(settings)
						.build();
		this.elasticsearchClient = initClient();
	}

	private static Settings
			getSettingBuilderWithClusterName(String clusterName) {
		return ImmutableSettings.settingsBuilder()
				.put(CLUSTER_NAME, clusterName).build();
	}

	private static Settings getSettingsWithClientTransportSniff(
			Settings settings, boolean clientTransportSniff) {
		return ImmutableSettings.settingsBuilder()
				.put(CLIENT_TRANSPORT_SNIFF, clientTransportSniff).put(settings)
				.build();
	}

	private Client initClient() {
		return this.isTransportClient ? buildTransportClient()
				: buildNodeClient();
	}

	private Client buildTransportClient() {
		TransportClient transportClient = new TransportClient(settings);
		for (String ipPort : ipPortListByComma.split(",")) {
			String[] seperatedIpPort = ipPort.split(":");
			transportClient.addTransportAddress(new InetSocketTransportAddress(
					seperatedIpPort[0], Integer.parseInt(seperatedIpPort[1])));
		}
		return transportClient;
	}

	private Client buildNodeClient() {
		return NodeBuilder.nodeBuilder().settings(settings).data(false)
				.client(true).build().client();
	}

	public IndexResponse sendData(String jsonSource, String index, String type,
			String id) {
		return sendDataToElasticsearch(
				prepareIndex(index, type, id).setSource(jsonSource));
	}

	public IndexResponse sendData(Map<String, Object> source, String index,
			String type, String id) {
		return sendDataToElasticsearch(
				prepareIndex(index, type, id).setSource(source));
	}

	public IndexResponse sendDataWithObjectMapper(Object sourceObject,
			String index, String type, String id) {
		return sendDataToElasticsearch(prepareIndex(index, type, id)
				.setSource(buildSourceByJsonMapper(sourceObject)));
	}

	public String sendData(String jsonSource, String index, String type) {
		return sendDataToElasticsearch(
				prepareIndex(index, type).setSource(jsonSource)).getId();
	}

	public String sendData(Map<String, Object> source, String index,
			String type) {
		return sendDataToElasticsearch(
				prepareIndex(index, type).setSource(source)).getId();
	}

	public String sendDataWithObjectMapper(Object sourceObject, String index,
			String type) {
		return sendDataToElasticsearch(prepareIndex(index, type)
				.setSource(buildSourceByJsonMapper(sourceObject))).getId();
	}

	private String buildSourceByJsonMapper(Object sourceObject) {
		try {
			return JsonMapper.writeValueAsString(sourceObject);
		} catch (JsonProcessingException e) {
			return JMExceptionManager.handleExceptionAndThrowRuntimeEx(log, e,
					"buildSourceByJsonMapper", sourceObject);
		}
	}

	private IndexResponse
			sendDataToElasticsearch(IndexRequestBuilder indexRequestBuilder) {
		return sendDataToElasticsearch(indexRequestBuilder, 0);
	}

	private IndexResponse sendDataToElasticsearch(
			IndexRequestBuilder indexRequestBuilder, int tryCount) {
		try {
			return indexRequestBuilder.setTTL(timeoutMillis).execute()
					.actionGet();
		} catch (Exception e) {
			if (e instanceof NoNodeAvailableException) {
				log.error(
						"initialize elasticsearchClient !!! tryCount = {}, indexRequestBuilder = {}",
						tryCount, indexRequestBuilder);
				close();
				initClient();
			}
			if (++tryCount > retryCount)
				throw new RuntimeException(
						"sendDataToElasticsearch Failure!!! over " + retryCount
								+ " times try, indexRequestBuilder = "
								+ indexRequestBuilder);
			log.warn(
					"{} Times Try To Send Data To Elasticsearch - exception = {}, indexRequestBuilder = {}",
					tryCount, e.toString(), indexRequestBuilder);
			return sendDataToElasticsearch(indexRequestBuilder, tryCount);
		}
	}

	public boolean isExists(String index) {
		return admin().indices().prepareExists(index).execute().actionGet()
				.isExists();
	}

	public boolean deleteIndices(String... indices) {
		return JMArrays.isNotNullOrEmpty(indices) ? admin().indices()
				.prepareDelete(indices).execute().actionGet().isAcknowledged()
				: false;
	}

	public DeleteByQueryResponse deleteAllDocs(String... indices) {
		return deleteDocs(QueryBuilders.matchAllQuery(), indices);
	}

	public DeleteByQueryResponse deleteDocs(QueryBuilder queryBuilder,
			String... indices) {
		return prepareDeleteByQuery(indices).setQuery(queryBuilder).execute()
				.actionGet();
	}

	public DeleteResponse deleteDoc(String index, String type, String id) {
		return prepareDelete(index, type, id).execute().actionGet();
	}

	public DeleteByQueryResponse deleteDocs(String[] indices, String[] types,
			QueryBuilder queryBuilder) {
		return prepareDeleteByQuery(indices).setTypes(types)
				.setQuery(queryBuilder).execute().actionGet();
	}

	public DeleteByQueryResponse deleteDocs(String[] indices, String[] types,
			FilterBuilder filterBuilder) {
		return deleteDocs(indices, types, QueryBuilders
				.filteredQuery(QueryBuilders.matchAllQuery(), filterBuilder));
	}

	public DeleteByQueryResponse deleteAllDocs(String[] indices,
			String[] types) {
		return prepareDeleteByQuery(indices)
				.setQuery(QueryBuilders.matchAllQuery()).setTypes(types)
				.execute().actionGet();
	}

	public boolean create(String index) {
		return admin().indices().prepareCreate(index).execute().actionGet()
				.isAcknowledged();
	}

	public long count(String... indices) {
		return prepareCount(indices).setQuery(QueryBuilders.matchAllQuery())
				.execute().actionGet().getCount();
	}

	public long count(String[] indices, String[] types) {
		return prepareCount(indices).setTypes(types).execute().actionGet()
				.getCount();
	}

	public SearchRequestBuilder getSearchRequestBuilder(boolean isSetExplain,
			String... indices) {
		return prepareSearch(indices)
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

	public SearchResponse search(SearchRequestBuilder searchRequestBuilder) {
		log.info("[Search Query DSL] - {}", searchRequestBuilder.toString());
		SearchResponse searchResponse =
				searchRequestBuilder.execute().actionGet();
		log.debug("[Search Response] - {}", searchResponse.toString());
		return searchResponse;
	}

	public SearchResponse searchAll(boolean isSetExplain, String... indices) {
		return search(
				getSearchRequestBuilderWithMatchAll(isSetExplain, indices));
	}

	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types) {
		return search(getSearchRequestBuilderWithMatchAll(isSetExplain, indices,
				types));
	}

	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, String[] fields) {
		return search(
				getSearchRequestBuilder(isSetExplain, indices, types, fields));
	}

	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, FilterBuilder filterBuilder) {
		return search(getSearchRequestBuilderWithMatchAll(isSetExplain, indices,
				types, filterBuilder));
	}

	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, String[] fields, FilterBuilder filterBuilder) {
		return search(getSearchRequestBuilderWithMatchAll(isSetExplain, indices,
				types, fields, filterBuilder));
	}

	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, AbstractAggregationBuilder... aggregationBuilders) {
		return search(getSearchRequestBuilderWithMatchAll(isSetExplain, indices,
				types, aggregationBuilders));
	}

	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, String[] fields,
			AbstractAggregationBuilder... aggregationBuilders) {
		return search(getSearchRequestBuilderWithMatchAll(isSetExplain, indices,
				types, fields, aggregationBuilders));
	}

	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, FilterBuilder filterBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return search(getSearchRequestBuilderWithMatchAll(isSetExplain, indices,
				types, filterBuilder, aggregationBuilders));
	}

	public SearchResponse searchAll(boolean isSetExplain, String[] indices,
			String[] types, String[] fields, FilterBuilder filterBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return search(getSearchRequestBuilderWithMatchAll(isSetExplain, indices,
				types, fields, filterBuilder, aggregationBuilders));
	}

	public SearchResponse searchAll(String... indices) {
		return searchAll(false, indices);
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
		return search(getSearchRequestBuilderWithMatchAll(false, indices, types)
				.setSize(((Long) count(indices, types)).intValue()));
	}

	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, String[] fields) {
		return search(getSearchRequestBuilderWithMatchAll(false, indices, types,
				fields).setSize(((Long) count(indices, types)).intValue()));
	}

	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, FilterBuilder filterBuilder) {
		return search(getSearchRequestBuilderWithMatchAll(false, indices, types,
				filterBuilder)
						.setSize(((Long) count(indices, types)).intValue()));
	}

	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, String[] fields, FilterBuilder filterBuilder) {
		return search(getSearchRequestBuilderWithMatchAll(false, indices, types,
				fields, filterBuilder)
						.setSize(((Long) count(indices, types)).intValue()));
	}

	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, AbstractAggregationBuilder... aggregationBuilders) {
		return search(getSearchRequestBuilderWithMatchAll(false, indices, types,
				aggregationBuilders)
						.setSize(((Long) count(indices, types)).intValue()));
	}

	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, String[] fields,
			AbstractAggregationBuilder... aggregationBuilders) {
		return search(getSearchRequestBuilderWithMatchAll(false, indices, types,
				fields, aggregationBuilders)
						.setSize(((Long) count(indices, types)).intValue()));
	}

	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, FilterBuilder filterBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return search(getSearchRequestBuilderWithMatchAll(false, indices, types,
				filterBuilder, aggregationBuilders)
						.setSize(((Long) count(indices, types)).intValue()));
	}

	public SearchResponse searchAllWithTargetCount(String[] indices,
			String[] types, String[] fields, FilterBuilder filterBuilder,
			AbstractAggregationBuilder... aggregationBuilders) {
		return search(getSearchRequestBuilderWithMatchAll(false, indices, types,
				fields, filterBuilder, aggregationBuilders)
						.setSize(((Long) count(indices, types)).intValue()));
	}

	public GetMappingsResponse getMappings(String... indices) {
		return admin().indices().prepareGetMappings(indices).execute()
				.actionGet();
	}

	public Set<String> getAllIndices() {
		try {
			return admin().indices().stats(new IndicesStatsRequest())
					.actionGet().getIndices().keySet();
		} catch (ElasticsearchException e) {
			return JMExceptionManager.handleExceptionAndReturn(log, e,
					"getAllIndices", Collections::emptySet);
		}
	}

	public Set<String> getIndexList(String containedString) {
		return getAllIndices().stream()
				.filter(index -> index.contains(containedString))
				.collect(toSet());
	}

}
