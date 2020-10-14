package kr.jm.utils.elasticsearch;

import kr.jm.utils.enums.OS;
import kr.jm.utils.exception.JMExceptionManager;
import kr.jm.utils.helper.JMLog;
import kr.jm.utils.helper.JMString;
import lombok.Getter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
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
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.*;

import static java.util.stream.Collectors.*;

/**
 * The type Jm elasticsearch client.
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
    private final Settings settings;
    @Delegate
    private final JMElasticsearchBulk jmESBulk;
    @Delegate
    private final JMElasticsearchIndex jmESIndex;
    @Delegate
    private final JMElasticsearchSearchAndCount jmESSearchAndCount;
    @Delegate
    private final JMElasticsearchDelete jmESDelete;

    /**
     * Instantiates a new Jm elasticsearch client.
     */
    public JMElasticsearchClient() {
        this(JMString.buildIpOrHostnamePortPair(OS.getHostname(), 9300));
    }

    /**
     * Instantiates a new Jm elasticsearch client.
     *
     * @param elasticsearchConnect the elasticsearch connect
     */
    public JMElasticsearchClient(String elasticsearchConnect) {
        this(elasticsearchConnect, OS.getHostname());
    }

    /**
     * Instantiates a new Jm elasticsearch client.
     *
     * @param elasticsearchConnect the elasticsearch connect
     * @param clientTransportSniff the client transport sniff
     */
    public JMElasticsearchClient(String elasticsearchConnect, boolean clientTransportSniff) {
        this(elasticsearchConnect, OS.getHostname(), clientTransportSniff);
    }

    /**
     * Instantiates a new Jm elasticsearch client.
     *
     * @param elasticsearchConnect the elasticsearch connect
     * @param clientTransportSniff the client transport sniff
     * @param clusterName          the cluster name
     */
    public JMElasticsearchClient(String elasticsearchConnect, boolean clientTransportSniff, String clusterName) {
        this(elasticsearchConnect, OS.getHostname(), clientTransportSniff, clusterName);
    }

    /**
     * Instantiates a new Jm elasticsearch client.
     *
     * @param elasticsearchConnect the elasticsearch connect
     * @param nodeName             the node name
     */
    public JMElasticsearchClient(String elasticsearchConnect, String nodeName) {
        this(elasticsearchConnect, nodeName, true);
    }

    /**
     * Instantiates a new Jm elasticsearch client.
     *
     * @param elasticsearchConnect the elasticsearch connect
     * @param nodeName             the node name
     * @param clientTransportSniff the client transport sniff
     */
    public JMElasticsearchClient(String elasticsearchConnect, String nodeName, boolean clientTransportSniff) {
        this(elasticsearchConnect, nodeName, clientTransportSniff, null);
    }

    /**
     * Instantiates a new Jm elasticsearch client.
     *
     * @param elasticsearchConnect the elasticsearch connect
     * @param nodeName             the node name
     * @param clientTransportSniff the client transport sniff
     * @param clusterName          the cluster name
     */
    public JMElasticsearchClient(String elasticsearchConnect, String nodeName, boolean clientTransportSniff,
            String clusterName) {
        this(elasticsearchConnect, getSettingsBuilder(nodeName, clientTransportSniff, clusterName).build());
    }

    /**
     * Instantiates a new Jm elasticsearch client.
     *
     * @param elasticsearchConnect the elasticsearch connect
     * @param settings             the settings
     */
    public JMElasticsearchClient(String elasticsearchConnect, Settings settings) {
        super(settings);
        this.settings = settings;
        try {
            for (String ipPort : elasticsearchConnect.split(",")) {
                String[] separatedIpPort = ipPort.split(":");
                addTransportAddress(new TransportAddress(InetAddress.getByName(separatedIpPort[0]),
                        Integer.parseInt(separatedIpPort[1])));
            }
        } catch (Exception e) {
            JMExceptionManager
                    .handleExceptionAndThrowRuntimeEx(log, e, "JMElasticsearchClient", elasticsearchConnect, settings);
        }
        JMLog.info(log, "initElasticsearchClient", elasticsearchConnect, settings);
        this.jmESBulk = new JMElasticsearchBulk(this);
        this.jmESIndex = new JMElasticsearchIndex(this);
        this.jmESSearchAndCount = new JMElasticsearchSearchAndCount(this);
        this.jmESDelete = new JMElasticsearchDelete(this);
    }

    /**
     * Gets settings builder.
     *
     * @param nodeName             the node name
     * @param clientTransportSniff the client transport sniff
     * @param clusterName          the cluster name
     * @return the settings builder
     */
    public static Builder getSettingsBuilder(String nodeName, boolean clientTransportSniff, String clusterName) {
        boolean isNullClusterName = Objects.isNull(clusterName);
        Builder builder = Settings.builder().put(NODE_NAME, nodeName).put(CLIENT_TRANSPORT_SNIFF, clientTransportSniff)
                .put(CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME, isNullClusterName);
        if (!isNullClusterName)
            builder.put(CLUSTER_NAME, clusterName);
        return builder;
    }

    /**
     * Is exists boolean.
     *
     * @param index the index
     * @return the boolean
     */
    public boolean isExists(String index) {
        IndicesExistsRequestBuilder indicesExistsRequestBuilder = admin().indices().prepareExists(index);
        return JMElasticsearchUtil.logRequestQueryAndReturn("isExists", indicesExistsRequestBuilder,
                indicesExistsRequestBuilder.execute()).isExists();
    }

    /**
     * Create boolean.
     *
     * @param index the index
     * @return the boolean
     */
    public boolean create(String index) {
        CreateIndexRequestBuilder createIndexRequestBuilder = admin().indices().prepareCreate(index);
        return JMElasticsearchUtil
                .logRequestQueryAndReturn("create", createIndexRequestBuilder, createIndexRequestBuilder.execute())
                .isAcknowledged();
    }

    /**
     * Extract id list list.
     *
     * @param searchResponse the search response
     * @return the list
     */
    public List<String> extractIdList(SearchResponse searchResponse) {
        return Arrays.stream(searchResponse.getHits().getHits()).map(SearchHit::getId).collect(toList());
    }

    /**
     * Gets all id list.
     *
     * @param index the index
     * @return the all id list
     */
    public List<String> getAllIdList(String index) {
        return extractIdList(searchAll(index));
    }

    /**
     * Extract id list list.
     *
     * @param index              the index
     * @param filterQueryBuilder the filter query builder
     * @return the list
     */
    public List<String> extractIdList(String index, QueryBuilder filterQueryBuilder) {
        return extractIdList(searchAllWithTargetCount(index, filterQueryBuilder));
    }

    /**
     * Gets mappings response.
     *
     * @param indices the indices
     * @return the mappings response
     */
    public ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetadata>> getMappingsResponse(String... indices) {
        GetMappingsRequestBuilder getMappingsRequestBuilder = admin().indices().prepareGetMappings(indices);
        return JMElasticsearchUtil.logRequestQueryAndReturn("getMappingsResponse", getMappingsRequestBuilder,
                getMappingsRequestBuilder.execute()).getMappings();
    }

    /**
     * Gets all indices stats.
     *
     * @return the all indices stats
     */
    public Map<String, IndexStats> getAllIndicesStats() {
        IndicesStatsRequestBuilder indicesStatsRequestBuilder = admin().indices().prepareStats().all();
        return JMElasticsearchUtil.logRequestQueryAndReturn("getAllIndicesStats", indicesStatsRequestBuilder,
                indicesStatsRequestBuilder.execute()).getIndices();
    }

    /**
     * Gets all indices.
     *
     * @return the all indices
     */
    public Set<String> getAllIndices() {
        return getAllIndicesStats().keySet();
    }

    /**
     * Gets filtered index list.
     *
     * @param containedString the contained string
     * @return the filtered index list
     */
    public List<String> getFilteredIndexList(String containedString) {
        return getAllIndices().stream().filter(index -> index.contains(containedString)).collect(toList());
    }

    /**
     * Gets query.
     *
     * @param getRequestBuilder the get request builder
     * @return the query
     */
    public GetResponse getQuery(GetRequestBuilder getRequestBuilder) {
        return JMElasticsearchUtil.logRequestQueryAndReturn("getQuery", getRequestBuilder, getRequestBuilder.execute());
    }

    /**
     * Update query update response.
     *
     * @param updateRequestBuilder the update request builder
     * @return the update response
     */
    public UpdateResponse updateQuery(UpdateRequestBuilder updateRequestBuilder) {
        return JMElasticsearchUtil
                .logRequestQueryAndReturn("updateQuery", updateRequestBuilder, updateRequestBuilder.execute());
    }

    /**
     * Gets mappings.
     *
     * @param index the index
     * @return the mappings
     */
    public Optional<Map<String, Object>> getMappings(String index) {
        try {
            return Optional.of(getMappingsResponse(index).get(index).get("_doc").getSourceAsMap());
        } catch (Exception e) {
            return JMExceptionManager.handleExceptionAndReturnEmptyOptional(log, e, "getMappings", index);
        }
    }

}
