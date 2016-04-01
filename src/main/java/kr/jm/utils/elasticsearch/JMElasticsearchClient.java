package kr.jm.utils.elasticsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
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
import org.elasticsearch.common.hppc.ObjectLookupContainer;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JMElasticsearchClient {

	private static final String CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME =
			"client.transport.ignore_cluster_name";
	private static final String CLUSTER_NAME = "cluster.name";
	private static final String CLIENT_TRANSPORT_SNIFF =
			"client.transport.sniff";

	@Delegate
	protected Client elasticsearchClient;
	private long SENDING_TIME_OUT;
	private int RETRY_THRESHOLD;
	private String ipNPortListByComma;
	private ObjectMapper jsonMapper;
	private Settings elasticsearchSettings;
	private int defaultPageSize;

	synchronized private TransportClient
			initElasticsearchClient(Settings settings) {
		TransportClient transportClient = new TransportClient(settings);
		String[] ipNPorts = ipNPortListByComma.split(",");
		for (String ipNPort : ipNPorts) {
			String[] seperateIpNPort = ipNPort.split(":");
			transportClient.addTransportAddress(new InetSocketTransportAddress(
					seperateIpNPort[0], Integer.parseInt(seperateIpNPort[1])));
		}
		return transportClient;
	}

	private Builder getSettingBuilder(boolean clientTransportSniff) {
		return ImmutableSettings.settingsBuilder().put(CLIENT_TRANSPORT_SNIFF,
				clientTransportSniff);
	}

	public JMElasticsearchClient(String clusterName, String ipNPortListByComma,
			boolean clientTransportSniff, int timeOutSeconds, int retryCount,
			int defaultHitsCount) {
		init(ipNPortListByComma, defaultHitsCount,
				getSettingBuilder(clientTransportSniff)
						.put(CLUSTER_NAME, clusterName).build(),
				retryCount, timeOutSeconds);
	}

	public JMElasticsearchClient(String clusterName, String ipNPortListByComma,
			boolean clientTransportSniff, int retryCount,
			int defaultHitsCount) {
		init(ipNPortListByComma, defaultHitsCount,
				getSettingBuilder(clientTransportSniff)
						.put(CLUSTER_NAME, clusterName).build(),
				retryCount);
	}

	public JMElasticsearchClient(String ipNPortListByComma,
			boolean clientTransportSniff, int timeOutSeconds, int retryCount,
			int defaultHitsCount) {
		init(ipNPortListByComma, defaultHitsCount,
				getSettingBuilder(clientTransportSniff)
						.put(CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME, true)
						.build(),
				retryCount, timeOutSeconds);
	}

	public JMElasticsearchClient(String ipNPortListByComma,
			boolean clientTransportSniff, int retryCount,
			int defaultHitsCount) {
		init(ipNPortListByComma, defaultHitsCount,
				getSettingBuilder(clientTransportSniff)
						.put(CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME, true)
						.build(),
				retryCount);
	}

	public JMElasticsearchClient(String clusterName, String ipNPortListByComma,
			int timeOutSeconds, int retryCount, int defaultHitsCount) {
		this(clusterName, ipNPortListByComma, true, timeOutSeconds, retryCount,
				defaultHitsCount);
	}

	public JMElasticsearchClient(String clusterName, String ipNPortListByComma,
			int retryCount, int defaultHitsCount) {
		this(clusterName, ipNPortListByComma, true, retryCount,
				defaultHitsCount);
	}

	public JMElasticsearchClient(String ipNPortListByComma, int timeOutSeconds,
			int retryCount, int defaultHitsCount) {
		init(ipNPortListByComma, defaultHitsCount,
				getSettingBuilder(true)
						.put(CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME, true)
						.build(),
				retryCount, timeOutSeconds);
	}

	public JMElasticsearchClient(String ipNPortListByComma, int retryCount,
			int defaultHitsCount) {
		init(ipNPortListByComma, defaultHitsCount,
				getSettingBuilder(true)
						.put(CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME, true)
						.build(),
				retryCount);
	}

	private void init(String ipNPortListByComma, int defaultPageSize,
			Settings settings, int retryCount, int timeOutSeconds) {
		this.ipNPortListByComma = ipNPortListByComma;
		this.elasticsearchClient = initElasticsearchClient(settings);
		this.RETRY_THRESHOLD = retryCount;
		this.defaultPageSize = defaultPageSize;
		this.jsonMapper = new ObjectMapper();
		this.SENDING_TIME_OUT = TimeUnit.SECONDS.toMillis(timeOutSeconds);
	}

	private void init(String ipNPortListByComma, int defaultPageSize,
			Settings settings, int retryCount) {
		init(ipNPortListByComma, defaultPageSize, settings, retryCount, 0);
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
			String index, String type, String id) throws JsonParseException,
					JsonMappingException, JsonProcessingException, IOException {
		return sendDataToElasticsearch(prepareIndex(index, type, id)
				.setSource(jsonMapper.writeValueAsString(sourceObject)));
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
			String type) throws JsonParseException, JsonMappingException,
					JsonProcessingException, IOException {
		return sendDataToElasticsearch(prepareIndex(index, type)
				.setSource(jsonMapper.writeValueAsString(sourceObject)))
						.getId();
	}

	private IndexResponse
			sendDataToElasticsearch(IndexRequestBuilder indexRequestBuilder) {
		return sendDataToElasticsearch(indexRequestBuilder, 0);
	}

	private IndexResponse sendDataToElasticsearch(
			IndexRequestBuilder indexRequestBuilder, int tryCount) {
		try {
			ListenableActionFuture<IndexResponse> execute =
					indexRequestBuilder.execute();
			return tryCount < 1 ? execute.actionGet()
					: execute.actionGet(SENDING_TIME_OUT);
		} catch (Exception e) {
			if (e instanceof NoNodeAvailableException) {
				log.error(
						"initialize elasticsearchClient !!! tryCount = {}, indexRequestBuilder = {}",
						tryCount, indexRequestBuilder);
				close();
				this.elasticsearchClient =
						initElasticsearchClient(elasticsearchSettings);
			}
			if (++tryCount > RETRY_THRESHOLD)
				throw new RuntimeException(
						"sendDataToElasticsearch Failure!!! over "
								+ RETRY_THRESHOLD
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
		return admin().indices().prepareDelete(indices).execute().actionGet()
				.isAcknowledged();
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
				.setSize(defaultPageSize).setExplain(isSetExplain);
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

	public List<String> getAllIndexList() {
		ArrayList<String> indexList = new ArrayList<String>();
		ObjectLookupContainer<String> keys =
				admin().cluster().prepareState().execute().actionGet()
						.getState().getMetaData().indices().keys();
		for (ObjectCursor<String> objectCursor : keys)
			indexList.add(objectCursor.value);
		Collections.sort(indexList);
		return indexList;
	}

	public List<String> getIndexList(String containedString) {
		ArrayList<String> indexList = new ArrayList<String>();
		for (String index : getAllIndexList())
			if (index.contains(containedString))
				indexList.add(index);
		return indexList;
	}

}
