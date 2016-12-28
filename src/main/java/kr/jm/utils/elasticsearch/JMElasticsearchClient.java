package kr.jm.utils.elasticsearch;

import static java.util.stream.Collectors.toList;

import java.net.InetAddress;
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
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import kr.jm.utils.enums.OS;
import kr.jm.utils.exception.JMExceptionManager;
import kr.jm.utils.helper.JMLog;
import kr.jm.utils.helper.JMString;
import lombok.Getter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

/**
 * The Class JMElasticsearchClient.
 */
@Slf4j
public class JMElasticsearchClient extends PreBuiltTransportClient {

	private static final String NODE_NAME = "node.name";
	private static final String CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME =
			"client.transport.ignore_cluster_name";
	private static final String CLUSTER_NAME = "cluster.name";
	private static final String CLIENT_TRANSPORT_SNIFF =
			"client.transport.sniff";

	@Getter
	private Settings settings;
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
		this(JMString.buildIpOrHostnamePortPair(OS.getHostname(), 9300));
	}

	/**
	 * Instantiates a new JM elasticsearch client.
	 *
	 * @param elasticsearchConnect
	 *            the elasticsearch connect
	 */
	public JMElasticsearchClient(String elasticsearchConnect) {
		this(elasticsearchConnect, OS.getHostname());
	}

	/**
	 * Instantiates a new JM elasticsearch client.
	 *
	 * @param elasticsearchConnect
	 *            the elasticsearch connect
	 * @param settings
	 *            the settings
	 */
	public JMElasticsearchClient(String elasticsearchConnect,
			Settings settings) {
		super(settings);
		try {
			for (String ipPort : elasticsearchConnect.split(",")) {
				String[] seperatedIpPort = ipPort.split(":");
				addTransportAddress(new InetSocketTransportAddress(
						InetAddress.getByName(seperatedIpPort[0]),
						Integer.parseInt(seperatedIpPort[1])));
			}
		} catch (Exception e) {
			JMExceptionManager.handleExceptionAndThrowRuntimeEx(log, e,
					"JMElasticsearchClient", elasticsearchConnect, settings);
		}
		JMLog.info(log, "initElasticsearchClient", elasticsearchConnect,
				settings);
		this.jmESBulk = new JMElasticsearchBulk(this);
		this.jmESIndex = new JMElasticsearchIndex(this);
		this.jmESSearchAndCount = new JMElasticsearchSearchAndCount(this);
		this.jmESDelete = new JMElasticsearchDelete(this);
	}

	/**
	 * Instantiates a new JM elasticsearch client.
	 *
	 * @param elasticsearchConnect
	 *            the elasticsearch connect
	 * @param nodeName
	 *            the node name
	 * @param clientTransportSniff
	 *            the client transport sniff
	 * @param ignoreClusterName
	 *            the ignore cluster name
	 */
	public JMElasticsearchClient(String elasticsearchConnect, String nodeName,
			boolean clientTransportSniff, boolean ignoreClusterName) {
		this(elasticsearchConnect, getSettingsBuilder(nodeName,
				clientTransportSniff, ignoreClusterName).build());
	}

	/**
	 * Instantiates a new JM elasticsearch client.
	 *
	 * @param elasticsearchConnect
	 *            the elasticsearch connect
	 * @param nodeName
	 *            the node name
	 * @param clientTransportSniff
	 *            the client transport sniff
	 */
	public JMElasticsearchClient(String elasticsearchConnect, String nodeName,
			boolean clientTransportSniff) {
		this(elasticsearchConnect, nodeName, clientTransportSniff, true);
	}

	/**
	 * Instantiates a new JM elasticsearch client.
	 *
	 * @param elasticsearchConnect
	 *            the elasticsearch connect
	 * @param nodeName
	 *            the node name
	 */
	public JMElasticsearchClient(String elasticsearchConnect, String nodeName) {
		this(elasticsearchConnect, nodeName, true);
	}

	/**
	 * Gets the settings builder.
	 *
	 * @param nodeName
	 *            the node name
	 * @param clusterName
	 *            the cluster name
	 * @param clientTransportSniff
	 *            the client transport sniff
	 * @return the settings builder
	 */
	public static Builder getSettingsBuilder(String nodeName,
			String clusterName, boolean clientTransportSniff) {
		return getSettingsBuilder(nodeName, clientTransportSniff, false)
				.put(CLUSTER_NAME, clusterName);
	}

	/**
	 * Gets the settings builder.
	 *
	 * @param nodeName
	 *            the node name
	 * @param clientTransportSniff
	 *            the client transport sniff
	 * @param ignoreClusterName
	 *            the ignore cluster name
	 * @return the settings builder
	 */
	public static Builder getSettingsBuilder(String nodeName,
			boolean clientTransportSniff, boolean ignoreClusterName) {
		return Settings.builder().put(NODE_NAME, nodeName)
				.put(CLIENT_TRANSPORT_SNIFF, clientTransportSniff)
				.put(CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME, ignoreClusterName);
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
	 * Extract id list.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @return the list
	 */
	public List<String> extractIdList(String index, String type,
			QueryBuilder filterQueryBuilder) {
		return extractIdList(
				searchAllWithTargetCount(index, type, filterQueryBuilder));
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
