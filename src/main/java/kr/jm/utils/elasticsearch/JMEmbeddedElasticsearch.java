package kr.jm.utils.elasticsearch;

import kr.jm.utils.enums.OS;
import kr.jm.utils.exception.JMExceptionManager;
import kr.jm.utils.helper.JMThread;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.percolator.PercolatorPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.transport.Netty4Plugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * The type Jm embedded elasticsearch.
 */
@Slf4j
public class JMEmbeddedElasticsearch extends Node {
    private static final Collection<Class<? extends Plugin>>
            PRE_INSTALLED_PLUGINS =
            Collections.unmodifiableList(
                    Arrays.asList(Netty4Plugin.class, ReindexPlugin.class,
                            PercolatorPlugin.class, MustachePlugin.class));

    /**
     * Instantiates a new Jm embedded elasticsearch.
     */
    public JMEmbeddedElasticsearch() {
        this(OS.getHostname(), "localhost");
    }

    /**
     * Instantiates a new Jm embedded elasticsearch.
     *
     * @param settings the settings
     */
    public JMEmbeddedElasticsearch(Settings settings) {
        super(InternalSettingsPreparer.prepareEnvironment(settings, null),
                PRE_INSTALLED_PLUGINS);
    }

    /**
     * Instantiates a new Jm embedded elasticsearch.
     *
     * @param nodeName    the node name
     * @param networkHost the network host
     */
    public JMEmbeddedElasticsearch(String nodeName, String networkHost) {
        this(getNodeConfig("JMEmbeddedElasticsearch", nodeName, networkHost,
                OS.getUserWorkingDir(), true).build());

    }

    /**
     * Gets node config.
     *
     * @param clusterName the cluster name
     * @param nodeName    the node name
     * @param networkHost the network host
     * @param homePath    the home path
     * @param nodeIngest  the node ingest
     * @return the node config
     */
    public static Builder getNodeConfig(String clusterName, String nodeName,
            String networkHost, String homePath, boolean nodeIngest) {
        return Settings.builder()
                // .put("path.metric", dataPath).put("http.port", httpRange)
                // .put("transport.tcp.port", transportRange)
                .put("node.name", nodeName).put("cluster.name", clusterName)
                .put("network.host", networkHost).put("path.home", homePath)
                .put("transport.type", "netty4")
                // .put("discovery.type", "none")
                .put("http.type", "netty4").put("node.ingest", nodeIngest);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.elasticsearch.node.Node#start()
     */
    @Override
    public Node start() {
        try {
            Node node = super.start();
            JMThread.sleep(1000);
            return node;
        } catch (NodeValidationException e) {
            return JMExceptionManager.handleExceptionAndThrowRuntimeEx(log, e,
                    "start");
        }
    }

    /**
     * Gets transport ip port pair.
     *
     * @return the transport ip port pair
     */
    public String getTransportIpPortPair() {
        return getCurrentNode().getTransport().address().publishAddress()
                .toString();
    }

    private NodeInfo getCurrentNode() {
        ClusterAdminClient cluster = client().admin().cluster();
        return cluster
                .prepareNodesInfo(cluster.prepareState().get().getState()
                        .getNodes().getLocalNodeId())
                .get().getNodes().iterator().next();
    }

    /**
     * Gets http ip port pair.
     *
     * @return the http ip port pair
     */
    public String getHttpIpPortPair() {
        return getCurrentNode().getTransport().address().publishAddress()
                .toString();
    }

}
