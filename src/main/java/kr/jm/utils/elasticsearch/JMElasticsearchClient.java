package kr.jm.utils.elasticsearch;

import static java.util.stream.Collectors.toList;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;

import lombok.Getter;
import lombok.experimental.Delegate;

/**
 * The Class JMElasticsearchClient.
 */
public class JMElasticsearchClient implements Client {

	private static final String LOCALHOST_9300 = "localhost:9300";
	private static final String NETWORK_HOST = "network.host";
	private static final String CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME =
			"client.transport.ignore_cluster_name";
	private static final String CLUSTER_NAME = "cluster.name";
	private static final String CLIENT_TRANSPORT_SNIFF =
			"client.transport.sniff";

	private String ipPortAsCsv;
	private boolean isTransportClient;

	@Getter
	private Settings settings;
	@Delegate
	private Client esClient;
	@Delegate
	private JMElasticsearchBulk jmESBulk;
	@Delegate
	private JMElasticsearchIndex jmESIndex;
	@Delegate
	private JMElasticsearchSearchAndCount jmESSearchAndCount;
	@Delegate
	private JMElasticsearchDelete jmESDelete;

	/**
	 * Instantiates a new JM elasticsearch client.
	 */
	public JMElasticsearchClient() {
		this(true, LOCALHOST_9300, getSettingBuilderWithIgnoreClusterName());
	}

	/**
	 * Instantiates a new JM elasticsearch client.
	 *
	 * @param elasticsearchClient
	 *            the elasticsearch client
	 */
	public JMElasticsearchClient(Client elasticsearchClient) {
		initElasticsearchClient(elasticsearchClient);
	}

	private void initElasticsearchClient(Client elasticsearchClient) {
		this.esClient = elasticsearchClient;
		this.jmESBulk = new JMElasticsearchBulk(this);
		this.jmESIndex = new JMElasticsearchIndex(this);
		this.jmESSearchAndCount = new JMElasticsearchSearchAndCount(this);
		this.jmESDelete = new JMElasticsearchDelete(this);
	}

	/**
	 * Instantiates a new JM elasticsearch client.
	 *
	 * @param ipPortAsCsv
	 *            the ip port as csv
	 */
	public JMElasticsearchClient(String ipPortAsCsv) {
		this(true, ipPortAsCsv, getSettingBuilderWithIgnoreClusterName());
	}

	/**
	 * Instantiates a new JM elasticsearch client.
	 *
	 * @param ipPortAsCsv
	 *            the ip port as csv
	 * @param clientTransportSniff
	 *            the client transport sniff
	 */
	public JMElasticsearchClient(String ipPortAsCsv,
			boolean clientTransportSniff) {
		this(true, ipPortAsCsv,
				getSettingsWithClientTransportSniff(
						getSettingBuilderWithIgnoreClusterName(),
						clientTransportSniff));
	}

	/**
	 * Instantiates a new JM elasticsearch client.
	 *
	 * @param ipPortAsCsv
	 *            the ip port as csv
	 * @param clientTransportSniff
	 *            the client transport sniff
	 * @param clusterName
	 *            the cluster name
	 */
	public JMElasticsearchClient(String ipPortAsCsv,
			boolean clientTransportSniff, String clusterName) {
		this(true, ipPortAsCsv,
				getSettingsWithClientTransportSniff(
						getSettingBuilderWithClusterName(clusterName),
						clientTransportSniff));
	}

	/**
	 * Instantiates a new JM elasticsearch client.
	 *
	 * @param isTransportClient
	 *            the is transport client
	 * @param ipPortAsCsv
	 *            the ip port as csv
	 */
	public JMElasticsearchClient(boolean isTransportClient,
			String ipPortAsCsv) {
		this(isTransportClient, ipPortAsCsv,
				getSettingBuilderWithIgnoreClusterName());
	}

	/**
	 * Instantiates a new JM elasticsearch client.
	 *
	 * @param isTransportClient
	 *            the is transport client
	 * @param ipPortAsCsv
	 *            the ip port as csv
	 * @param clientTransportSniff
	 *            the client transport sniff
	 */
	public JMElasticsearchClient(boolean isTransportClient, String ipPortAsCsv,
			boolean clientTransportSniff) {
		this(true, ipPortAsCsv,
				getSettingsWithClientTransportSniff(
						getSettingBuilderWithIgnoreClusterName(),
						clientTransportSniff));
	}

	private static Settings getSettingBuilderWithIgnoreClusterName() {
		return ImmutableSettings.settingsBuilder()
				.put(CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME, true).build();
	}

	/**
	 * Instantiates a new JM elasticsearch client.
	 *
	 * @param isTransportClient
	 *            the is transport client
	 * @param ipPortAsCsv
	 *            the ip port as csv
	 * @param clusterName
	 *            the cluster name
	 */
	public JMElasticsearchClient(boolean isTransportClient, String ipPortAsCsv,
			String clusterName) {
		this(isTransportClient, ipPortAsCsv,
				getSettingBuilderWithClusterName(clusterName));
	}

	/**
	 * Instantiates a new JM elasticsearch client.
	 *
	 * @param isTransportClient
	 *            the is transport client
	 * @param ipPortAsCsv
	 *            the ip port as csv
	 * @param clientTransportSniff
	 *            the client transport sniff
	 * @param clusterName
	 *            the cluster name
	 */
	public JMElasticsearchClient(boolean isTransportClient, String ipPortAsCsv,
			boolean clientTransportSniff, String clusterName) {
		this(isTransportClient, ipPortAsCsv,
				getSettingsWithClientTransportSniff(
						getSettingBuilderWithClusterName(clusterName),
						clientTransportSniff));
	}

	/**
	 * Instantiates a new JM elasticsearch client.
	 *
	 * @param isTransportClient
	 *            the is transport client
	 * @param ipPortAsCsv
	 *            the ip port as csv
	 * @param settings
	 *            the settings
	 */
	public JMElasticsearchClient(boolean isTransportClient, String ipPortAsCsv,
			Settings settings) {
		this.isTransportClient = isTransportClient;
		this.ipPortAsCsv = ipPortAsCsv;
		this.settings =
				isTransportClient ? settings
						: ImmutableSettings.settingsBuilder()
								.put(NETWORK_HOST, ipPortAsCsv).put(settings)
								.build();
		initElasticsearchClient(buildClient());
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

	private Client buildClient() {
		return this.isTransportClient ? buildTransportClient()
				: buildNodeClient();
	}

	private Client buildTransportClient() {
		TransportClient transportClient = new TransportClient(settings);
		for (String ipPort : ipPortAsCsv.split(",")) {
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

	/**
	 * Checks if is exists.
	 *
	 * @param index
	 *            the index
	 * @return true, if is exists
	 */
	public boolean isExists(String index) {
		IndicesExistsRequestBuilder indicesExistsRequestBuilder =
				admin().indices().prepareExists(index);
		return JMElastricsearchUtil
				.logExcuteAndReturn("isExists", indicesExistsRequestBuilder,
						indicesExistsRequestBuilder.execute())
				.isExists();
	}

	/**
	 * Creates the.
	 *
	 * @param index
	 *            the index
	 * @return true, if successful
	 */
	public boolean create(String index) {
		CreateIndexRequestBuilder createIndexRequestBuilder =
				admin().indices().prepareCreate(index);
		return JMElastricsearchUtil.logExcuteAndReturn("create",
				createIndexRequestBuilder, createIndexRequestBuilder.execute())
				.isAcknowledged();
	}

	/**
	 * Extract id list.
	 *
	 * @param searchResponse
	 *            the search response
	 * @return the list
	 */
	public List<String> extractIdList(SearchResponse searchResponse) {
		return Arrays.stream(searchResponse.getHits().hits())
				.map(SearchHit::getId).collect(toList());
	}

	/**
	 * Gets the all id list.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @return the all id list
	 */
	public List<String> getAllIdList(String index, String type) {
		return extractIdList(searchAllWithField(index, type, "_id"));
	}

	/**
	 * Gets the mappings response.
	 *
	 * @param indices
	 *            the indices
	 * @return the mappings response
	 */
	public ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>>
			getMappingsResponse(String... indices) {
		GetMappingsRequestBuilder getMappingsRequestBuilder =
				admin().indices().prepareGetMappings(indices);
		return JMElastricsearchUtil.logExcuteAndReturn("getMappingsResponse",
				getMappingsRequestBuilder, getMappingsRequestBuilder.execute())
				.getMappings();
	}

	/**
	 * Gets the all indices stats.
	 *
	 * @return the all indices stats
	 */
	public Map<String, IndexStats> getAllIndicesStats() {
		IndicesStatsRequestBuilder indicesStatsRequestBuilder =
				admin().indices().prepareStats().all();
		return JMElastricsearchUtil.logExcuteAndReturn("getAllIndicesStats",
				indicesStatsRequestBuilder,
				indicesStatsRequestBuilder.execute()).getIndices();
	}

	/**
	 * Gets the all indices.
	 *
	 * @return the all indices
	 */
	public Set<String> getAllIndices() {
		return getAllIndicesStats().keySet();
	}

	/**
	 * Gets the filtered index list.
	 *
	 * @param containedString
	 *            the contained string
	 * @return the filtered index list
	 */
	public List<String> getFilteredIndexList(String containedString) {
		return getAllIndices().stream()
				.filter(index -> index.contains(containedString))
				.collect(toList());
	}

	/**
	 * Gets the query.
	 *
	 * @param getRequestBuilder
	 *            the get request builder
	 * @return the query
	 */
	public GetResponse getQuery(GetRequestBuilder getRequestBuilder) {
		return JMElastricsearchUtil.logExcuteAndReturn("getQuery",
				getRequestBuilder, getRequestBuilder.execute());
	}

	/**
	 * Update query.
	 *
	 * @param updateRequestBuilder
	 *            the update request builder
	 * @return the update response
	 */
	public UpdateResponse
			updateQuery(UpdateRequestBuilder updateRequestBuilder) {
		return JMElastricsearchUtil.logExcuteAndReturn("updateQuery",
				updateRequestBuilder, updateRequestBuilder.execute());
	}

}
