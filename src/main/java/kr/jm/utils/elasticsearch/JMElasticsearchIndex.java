package kr.jm.utils.elasticsearch;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Map;

/**
 * The type Jm elasticsearch index.
 */
public class JMElasticsearchIndex {

    private final Client jmESClient;

    /**
     * Instantiates a new Jm elasticsearch index.
     *
     * @param elasticsearchClient the elasticsearch client
     */
    public JMElasticsearchIndex(Client elasticsearchClient) {
        this.jmESClient = elasticsearchClient;
    }

    /**
     * Index query index response.
     *
     * @param indexRequestBuilder the index request builder
     * @return the index response
     */
    public IndexResponse indexQuery(IndexRequestBuilder indexRequestBuilder) {
        return JMElasticsearchUtil
                .logRequestQueryAndReturn("indexQuery", indexRequestBuilder, indexRequestBuilder.execute());
    }

    /**
     * Index query async action future.
     *
     * @param indexRequestBuilder the index request builder
     * @return the action future
     */
    public ActionFuture<IndexResponse> indexQueryAsync(IndexRequestBuilder indexRequestBuilder) {
        return JMElasticsearchUtil.logRequestQuery("indexQueryAsync", indexRequestBuilder).execute();
    }

    private IndexRequestBuilder buildIndexRequest(String index, String id, String jsonSource) {
        return buildIndexRequest(index, id, JMElasticsearchUtil.buildSourceByJsonMapper(jsonSource));
    }

    private IndexRequestBuilder buildIndexRequest(String index, String id, Map<String, Object> source) {
        return getPrepareIndex(index, id).setSource(source);
    }

    private IndexRequestBuilder getPrepareIndex(String index, String id) {
        return id == null ? jmESClient.prepareIndex().setIndex(index) : jmESClient.prepareIndex().setIndex(index)
                .setId(id);
    }

    private UpdateRequestBuilder buildPrepareUpsert(String index, String id, String jsonString) {
        return jmESClient.prepareUpdate().setIndex(index).setId(id).setDoc(jsonString, XContentType.JSON)
                .setUpsert(new IndexRequest(index).id(id).source(jsonString, XContentType.JSON));
    }

    private UpdateRequestBuilder buildPrepareUpsert(String index, String id, Map<String, Object> source) {
        return jmESClient.prepareUpdate().setIndex(index).setId(id).setDoc(source)
                .setUpsert(new IndexRequest(index).id(id).source(source));
    }

    /**
     * Upsert query update response.
     *
     * @param updateRequestBuilder the update request builder
     * @return the update response
     */
    public UpdateResponse upsertQuery(UpdateRequestBuilder updateRequestBuilder) {
        return JMElasticsearchUtil.logRequestQueryAndReturn("upsertQuery",
                updateRequestBuilder, updateRequestBuilder.execute());
    }

    /**
     * Upsert query async action future.
     *
     * @param updateRequestBuilder the update request builder
     * @return the action future
     */
    public ActionFuture<UpdateResponse> upsertQueryAsync(UpdateRequestBuilder updateRequestBuilder) {
        return JMElasticsearchUtil.logRequestQuery("upsertQueryAsync", updateRequestBuilder).execute();
    }

    /**
     * Upsert data update response.
     *
     * @param index  the index
     * @param id     the id
     * @param source the source
     * @return the update response
     */
    public UpdateResponse upsertData(String index, String id, Map<String, Object> source) {
        return upsertQuery(buildPrepareUpsert(index, id, source));
    }

    /**
     * Upsert data async action future.
     *
     * @param index  the index
     * @param id     the id
     * @param source the source
     * @return the action future
     */
    public ActionFuture<UpdateResponse> upsertDataAsync(String index, String id, Map<String, Object> source) {
        return upsertQueryAsync(buildPrepareUpsert(index, id, source));
    }

    /**
     * Upsert data update response.
     *
     * @param index      the index
     * @param id         the id
     * @param jsonSource the json source
     * @return the update response
     */
    public UpdateResponse upsertData(String index, String id, String jsonSource) {
        return upsertQuery(buildPrepareUpsert(index, id, jsonSource));
    }

    /**
     * Upsert data async action future.
     *
     * @param index      the index
     * @param id         the id
     * @param jsonSource the json source
     * @return the action future
     */
    public ActionFuture<UpdateResponse> upsertDataAsync(String index, String id, String jsonSource) {
        return upsertQueryAsync(buildPrepareUpsert(index, id, jsonSource));
    }

    /**
     * Upsert data with object mapper update response.
     *
     * @param index        the index
     * @param id           the id
     * @param sourceObject the source object
     * @return the update response
     */
    public UpdateResponse upsertDataWithObjectMapper(String index, String id, Object sourceObject) {
        return upsertData(index, id, JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject));
    }

    /**
     * Upsert data a sync with object mapper action future.
     *
     * @param index        the index
     * @param id           the id
     * @param sourceObject the source object
     * @return the action future
     */
    public ActionFuture<UpdateResponse> upsertDataASyncWithObjectMapper(String index, String id, Object sourceObject) {
        return upsertDataAsync(index, id, JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject));
    }

    /**
     * Send data index response.
     *
     * @param index  the index
     * @param id     the id
     * @param source the source
     * @return the index response
     */
    public IndexResponse sendData(String index, String id, Map<String, Object> source) {
        return indexQuery(buildIndexRequest(index, id, source));
    }

    /**
     * Send data string.
     *
     * @param index  the index
     * @param source the source
     * @return the string
     */
    public String sendData(String index, Map<String, Object> source) {
        return sendData(index, null, source).getId();
    }

    /**
     * Send data index response.
     *
     * @param index      the index
     * @param id         the id
     * @param jsonSource the json source
     * @return the index response
     */
    public IndexResponse sendData(String index, String id, String jsonSource) {
        return indexQuery(buildIndexRequest(index, id, jsonSource));
    }

    /**
     * Send data string.
     *
     * @param index      the index
     * @param jsonSource the json source
     * @return the string
     */
    public String sendData(String index, String jsonSource) {
        return sendData(index, null, jsonSource).getId();
    }

    /**
     * Send data with object mapper index response.
     *
     * @param index        the index
     * @param id           the id
     * @param sourceObject the source object
     * @return the index response
     */
    public IndexResponse sendDataWithObjectMapper(String index, String id, Object sourceObject) {
        return sendData(index, id, JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject));
    }

    /**
     * Send data with object mapper string.
     *
     * @param index        the index
     * @param sourceObject the source object
     * @return the string
     */
    public String sendDataWithObjectMapper(String index, Object sourceObject) {
        return sendDataWithObjectMapper(index, null, sourceObject).getId();
    }

    /**
     * Send data async action future.
     *
     * @param index  the index
     * @param id     the id
     * @param source the source
     * @return the action future
     */
    public ActionFuture<IndexResponse> sendDataAsync(String index, String id, Map<String, Object> source) {
        return indexQueryAsync(buildIndexRequest(index, id, source));
    }

    /**
     * Send data async action future.
     *
     * @param index  the index
     * @param source the source
     * @return the action future
     */
    public ActionFuture<IndexResponse> sendDataAsync(String index, Map<String, Object> source) {
        return indexQueryAsync(buildIndexRequest(index, null, source));
    }

    /**
     * Send data async action future.
     *
     * @param index      the index
     * @param id         the id
     * @param jsonSource the json source
     * @return the action future
     */
    public ActionFuture<IndexResponse> sendDataAsync(String index, String id, String jsonSource) {
        return indexQueryAsync(buildIndexRequest(index, id, jsonSource));
    }

    /**
     * Send data async action future.
     *
     * @param index      the index
     * @param jsonSource the json source
     * @return the action future
     */
    public ActionFuture<IndexResponse> sendDataAsync(String index, String jsonSource) {
        return indexQueryAsync(buildIndexRequest(index, null, jsonSource));
    }

    /**
     * Send data async with object mapper action future.
     *
     * @param index        the index
     * @param id           the id
     * @param sourceObject the source object
     * @return the action future
     */
    public ActionFuture<IndexResponse> sendDataAsyncWithObjectMapper(String index, String id, Object sourceObject) {
        return indexQueryAsync(buildIndexRequest(index, id, JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject)));
    }

    /**
     * Send data async with object mapper action future.
     *
     * @param index        the index
     * @param sourceObject the source object
     * @return the action future
     */
    public ActionFuture<IndexResponse> sendDataAsyncWithObjectMapper(String index, Object sourceObject) {
        return indexQueryAsync(
                buildIndexRequest(index, null, JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject)));
    }

}
