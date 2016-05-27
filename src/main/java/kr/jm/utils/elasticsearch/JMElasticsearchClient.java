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
import lombok.Setter;
import lombok.experimental.Delegate;

public class JMElasticsearchClient implements Client {

	private static final String LOCALHOST_9300 = "localhost:9300";
	private static final String NETWORK_HOST = "network.host";
	private static final String CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME =
			"client.transport.ignore_cluster_name";
	private static final String CLUSTER_NAME = "cluster.name";
	private static final String CLIENT_TRANSPORT_SNIFF =
			"client.transport.sniff";

	@Delegate
	private JMElasticsearchBulk jmESBulk = new JMElasticsearchBulk(this);
	@Delegate
	private JMElasticsearchIndex jmESIndex = new JMElasticsearchIndex(this);
	@Delegate
	private JMElasticsearchSearchAndCount jmESSearchAndCount =
			new JMElasticsearchSearchAndCount(this);
	@Delegate
	private JMElasticsearchDelete jmESDelete = new JMElasticsearchDelete(this);

	private String ipPortListByComma;
	private boolean isTransportClient;
	@Getter
	private Settings settings;

	@Delegate
	private Client elasticsearchClient;

	@Getter
	@Setter
	static long timeoutMillis = 5000;

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

	public boolean isExists(String index) {
		IndicesExistsRequestBuilder indicesExistsRequestBuilder =
				admin().indices().prepareExists(index);
		return JMElastricsearchUtil
				.logExcuteAndReturn("isExists", indicesExistsRequestBuilder,
						indicesExistsRequestBuilder.execute())
				.isExists();
	}

	public boolean create(String index) {
		CreateIndexRequestBuilder createIndexRequestBuilder =
				admin().indices().prepareCreate(index);
		return JMElastricsearchUtil.logExcuteAndReturn("create",
				createIndexRequestBuilder, createIndexRequestBuilder.execute())
				.isAcknowledged();
	}

	public List<String> extractIdList(SearchResponse searchResponse) {
		return Arrays.stream(searchResponse.getHits().hits())
				.map(SearchHit::getId).collect(toList());
	}

	public List<String> getAllIdList(String index, String type) {
		return extractIdList(searchAllWithField(index, type, "_id"));
	}

	public ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>>
			getMappingsResponse(String... indices) {
		GetMappingsRequestBuilder getMappingsRequestBuilder =
				admin().indices().prepareGetMappings(indices);
		return JMElastricsearchUtil.logExcuteAndReturn("getMappingsResponse",
				getMappingsRequestBuilder, getMappingsRequestBuilder.execute())
				.getMappings();
	}

	public Map<String, IndexStats> getAllIndicesStats() {
		IndicesStatsRequestBuilder indicesStatsRequestBuilder =
				admin().indices().prepareStats().all();
		return JMElastricsearchUtil.logExcuteAndReturn("getAllIndicesStats",
				indicesStatsRequestBuilder,
				indicesStatsRequestBuilder.execute()).getIndices();
	}

	public Set<String> getAllIndices() {
		return getAllIndicesStats().keySet();
	}

	public List<String> getFilteredIndexList(String containedString) {
		return getAllIndices().stream()
				.filter(index -> index.contains(containedString))
				.collect(toList());
	}

	public GetResponse getQuery(GetRequestBuilder getRequestBuilder) {
		return JMElastricsearchUtil.logExcuteAndReturn("getQuery",
				getRequestBuilder, getRequestBuilder.execute());
	}

	public UpdateResponse
			updateQuery(UpdateRequestBuilder updateRequestBuilder) {
		return JMElastricsearchUtil.logExcuteAndReturn("updateQuery",
				updateRequestBuilder, updateRequestBuilder.execute());
	}

}
