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

    private IndexRequestBuilder buildIndexRequest(String index, String type, String id, String jsonSource) {
        return buildIndexRequest(index, type, id, JMElasticsearchUtil.buildSourceByJsonMapper(jsonSource));
    }

    private IndexRequestBuilder buildIndexRequest(String index, String type, String id, Map<String, Object> source) {
        return getPrepareIndex(index, type, id).setSource(source);
    }

    private IndexRequestBuilder getPrepareIndex(String index, String type, String id) {
        return id == null ? jmESClient.prepareIndex(index, type) : jmESClient.prepareIndex(index, type, id);
    }

    private UpdateRequestBuilder buildPrepareUpsert(String index, String type, String id, String jsonString) {
        return jmESClient.prepareUpdate(index, type, id).setDoc(jsonString, XContentType.JSON)
                .setUpsert(new IndexRequest(index, type, id).source(jsonString, XContentType.JSON));
    }

    private UpdateRequestBuilder buildPrepareUpsert(String index, String type,
            String id, Map<String, Object> source) {
        return jmESClient.prepareUpdate(index, type, id).setDoc(source)
                .setUpsert(new IndexRequest(index, type, id).source(source));
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
        return JMElasticsearchUtil.logRequestQuery("upsertQueryAsync",
                updateRequestBuilder
        ).execute();
    }

    /**
     * Upsert data update response.
     *
     * @param index  the index
     * @param type   the type
     * @param id     the id
     * @param source the source
     * @return the update response
     */
    public UpdateResponse upsertData(String index, String type, String id, Map<String, Object> source) {
        return upsertQuery(buildPrepareUpsert(index, type, id, source));
    }

    /**
     * Upsert data async action future.
     *
     * @param index  the index
     * @param type   the type
     * @param id     the id
     * @param source the source
     * @return the action future
     */
    public ActionFuture<UpdateResponse> upsertDataAsync(String index, String type, String id, Map<String, Object> source) {
        return upsertQueryAsync(buildPrepareUpsert(index, type, id, source));
    }

    /**
     * Upsert data update response.
     *
     * @param index      the index
     * @param type       the type
     * @param id         the id
     * @param jsonSource the json source
     * @return the update response
     */
    public UpdateResponse upsertData(String index, String type, String id, String jsonSource) {
        return upsertQuery(buildPrepareUpsert(index, type, id, jsonSource));
    }

    /**
     * Upsert data async action future.
     *
     * @param index      the index
     * @param type       the type
     * @param id         the id
     * @param jsonSource the json source
     * @return the action future
     */
    public ActionFuture<UpdateResponse> upsertDataAsync(String index, String type, String id, String jsonSource) {
        return upsertQueryAsync(buildPrepareUpsert(index, type, id, jsonSource));
    }

    /**
     * Upsert data with object mapper update response.
     *
     * @param index        the index
     * @param type         the type
     * @param id           the id
     * @param sourceObject the source object
     * @return the update response
     */
    public UpdateResponse upsertDataWithObjectMapper(String index, String type, String id, Object sourceObject) {
        return upsertData(index, type, id, JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject));
    }

    /**
     * Upsert data a sync with object mapper action future.
     *
     * @param index        the index
     * @param type         the type
     * @param id           the id
     * @param sourceObject the source object
     * @return the action future
     */
    public ActionFuture<UpdateResponse> upsertDataASyncWithObjectMapper(String index, String type, String id,
            Object sourceObject) {
        return upsertDataAsync(index, type, id, JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject));
    }

    /**
     * Send data index response.
     *
     * @param index  the index
     * @param type   the type
     * @param id     the id
     * @param source the source
     * @return the index response
     */
    public IndexResponse sendData(String index, String type, String id, Map<String, Object> source) {
        return indexQuery(buildIndexRequest(index, type, id, source));
    }

    /**
     * Send data string.
     *
     * @param index  the index
     * @param type   the type
     * @param source the source
     * @return the string
     */
    public String sendData(String index, String type, Map<String, Object> source) {
        return sendData(index, type, null, source).getId();
    }

    /**
     * Send data index response.
     *
     * @param index      the index
     * @param type       the type
     * @param id         the id
     * @param jsonSource the json source
     * @return the index response
     */
    public IndexResponse sendData(String index, String type, String id, String jsonSource) {
        return indexQuery(buildIndexRequest(index, type, id, jsonSource));
    }

    /**
     * Send data string.
     *
     * @param index      the index
     * @param type       the type
     * @param jsonSource the json source
     * @return the string
     */
    public String sendData(String index, String type, String jsonSource) {
        return sendData(index, type, null, jsonSource).getId();
    }

    /**
     * Send data with object mapper index response.
     *
     * @param index        the index
     * @param type         the type
     * @param id           the id
     * @param sourceObject the source object
     * @return the index response
     */
    public IndexResponse sendDataWithObjectMapper(String index, String type, String id, Object sourceObject) {
        return sendData(index, type, id, JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject));
    }

    /**
     * Send data with object mapper string.
     *
     * @param index        the index
     * @param type         the type
     * @param sourceObject the source object
     * @return the string
     */
    public String sendDataWithObjectMapper(String index, String type, Object sourceObject) {
        return sendDataWithObjectMapper(index, type, null, sourceObject).getId();
    }

    /**
     * Send data async action future.
     *
     * @param index  the index
     * @param type   the type
     * @param id     the id
     * @param source the source
     * @return the action future
     */
    public ActionFuture<IndexResponse> sendDataAsync(String index, String type, String id, Map<String, Object> source) {
        return indexQueryAsync(buildIndexRequest(index, type, id, source));
    }

    /**
     * Send data async action future.
     *
     * @param index  the index
     * @param type   the type
     * @param source the source
     * @return the action future
     */
    public ActionFuture<IndexResponse> sendDataAsync(String index, String type, Map<String, Object> source) {
        return indexQueryAsync(buildIndexRequest(index, type, null, source));
    }

    /**
     * Send data async action future.
     *
     * @param index      the index
     * @param type       the type
     * @param id         the id
     * @param jsonSource the json source
     * @return the action future
     */
    public ActionFuture<IndexResponse> sendDataAsync(String index, String type, String id, String jsonSource) {
        return indexQueryAsync(buildIndexRequest(index, type, id, jsonSource));
    }

    /**
     * Send data async action future.
     *
     * @param index      the index
     * @param type       the type
     * @param jsonSource the json source
     * @return the action future
     */
    public ActionFuture<IndexResponse> sendDataAsync(String index, String type, String jsonSource) {
        return indexQueryAsync(buildIndexRequest(index, type, null, jsonSource));
    }

    /**
     * Send data async with object mapper action future.
     *
     * @param index        the index
     * @param type         the type
     * @param id           the id
     * @param sourceObject the source object
     * @return the action future
     */
    public ActionFuture<IndexResponse> sendDataAsyncWithObjectMapper(String index, String type, String id,
            Object sourceObject) {
        return indexQueryAsync(
                buildIndexRequest(index, type, id, JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject)));
    }

    /**
     * Send data async with object mapper action future.
     *
     * @param index        the index
     * @param type         the type
     * @param sourceObject the source object
     * @return the action future
     */
    public ActionFuture<IndexResponse> sendDataAsyncWithObjectMapper(String index, String type, Object sourceObject) {
        return indexQueryAsync(
                buildIndexRequest(index, type, null, JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject)));
    }

}
