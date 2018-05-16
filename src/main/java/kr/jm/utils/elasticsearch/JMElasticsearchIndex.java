package kr.jm.utils.elasticsearch;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;

import java.util.Map;

/**
 * The type Jm elasticsearch index.
 */
public class JMElasticsearchIndex {

    private Client jmESClient;

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
        return JMElasticsearchUtil.logRequestQueryAndReturn("indexQuery",
                indexRequestBuilder, indexRequestBuilder.execute());
    }

    /**
     * Index query async action future.
     *
     * @param indexRequestBuilder the index request builder
     * @return the action future
     */
    public ActionFuture<IndexResponse> indexQueryAsync(
            IndexRequestBuilder indexRequestBuilder) {
        return JMElasticsearchUtil.logRequestQuery("indexQueryAsync",
                indexRequestBuilder
        ).execute();
    }

    private IndexRequestBuilder buildIndexRequest(String jsonSource,
            String index, String type, String id) {
        return buildIndexRequest(
                JMElasticsearchUtil.buildSourceByJsonMapper(jsonSource), index,
                type, id);
    }

    private IndexRequestBuilder buildIndexRequest(Map<String, ?> source,
            String index, String type, String id) {
        return getPrepareIndex(index, type, id).setSource(source);
    }

    private IndexRequestBuilder getPrepareIndex(String index, String type,
            String id) {
        return id == null ? jmESClient.prepareIndex(index, type)
                : jmESClient.prepareIndex(index, type, id);
    }

    private UpdateRequestBuilder buildPrepareUpsert(String index, String type,
            String id, String jsonString) {
        return jmESClient.prepareUpdate(index, type, id).setDoc(jsonString)
                .setUpsert(
                        new IndexRequest(index, type, id).source(jsonString));
    }

    private UpdateRequestBuilder buildPrepareUpsert(String index, String type,
            String id, Map<String, ?> source) {
        return jmESClient.prepareUpdate(index, type, id).setDoc(source)
                .setUpsert(new IndexRequest(index, type, id).source(source));
    }

    /**
     * Upsert query update response.
     *
     * @param updateRequestBuilder the update request builder
     * @return the update response
     */
    public UpdateResponse
    upsertQuery(UpdateRequestBuilder updateRequestBuilder) {
        return JMElasticsearchUtil.logRequestQueryAndReturn("upsertQuery",
                updateRequestBuilder, updateRequestBuilder.execute());
    }

    /**
     * Upsert query async action future.
     *
     * @param updateRequestBuilder the update request builder
     * @return the action future
     */
    public ActionFuture<UpdateResponse>
    upsertQueryAsync(UpdateRequestBuilder updateRequestBuilder) {
        return JMElasticsearchUtil.logRequestQuery("upsertQueryAsync",
                updateRequestBuilder
        ).execute();
    }

    /**
     * Upsert data update response.
     *
     * @param source the source
     * @param index  the index
     * @param type   the type
     * @param id     the id
     * @return the update response
     */
    public UpdateResponse upsertData(Map<String, ?> source, String index,
            String type, String id) {
        return upsertQuery(buildPrepareUpsert(index, type, id, source));
    }

    /**
     * Upsert data async action future.
     *
     * @param source the source
     * @param index  the index
     * @param type   the type
     * @param id     the id
     * @return the action future
     */
    public ActionFuture<UpdateResponse> upsertDataAsync(
            Map<String, ?> source, String index, String type, String id) {
        return upsertQueryAsync(buildPrepareUpsert(index, type, id, source));
    }

    /**
     * Upsert data update response.
     *
     * @param jsonSource the json source
     * @param index      the index
     * @param type       the type
     * @param id         the id
     * @return the update response
     */
    public UpdateResponse upsertData(String jsonSource, String index,
            String type, String id) {
        return upsertQuery(buildPrepareUpsert(index, type, id, jsonSource));
    }

    /**
     * Upsert data async action future.
     *
     * @param jsonSource the json source
     * @param index      the index
     * @param type       the type
     * @param id         the id
     * @return the action future
     */
    public ActionFuture<UpdateResponse> upsertDataAsync(
            String jsonSource, String index, String type, String id) {
        return upsertQueryAsync(
                buildPrepareUpsert(index, type, id, jsonSource));
    }

    /**
     * Upsert data with object mapper update response.
     *
     * @param sourceObject the source object
     * @param index        the index
     * @param type         the type
     * @param id           the id
     * @return the update response
     */
    public UpdateResponse upsertDataWithObjectMapper(Object sourceObject,
            String index, String type, String id) {
        return upsertData(
                JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject),
                index, type, id);
    }

    /**
     * Upsert data a sync with object mapper action future.
     *
     * @param sourceObject the source object
     * @param index        the index
     * @param type         the type
     * @param id           the id
     * @return the action future
     */
    public ActionFuture<UpdateResponse>
    upsertDataASyncWithObjectMapper(Object sourceObject, String index,
            String type, String id) {
        return upsertDataAsync(
                JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject),
                index, type, id);
    }

    /**
     * Send data index response.
     *
     * @param source the source
     * @param index  the index
     * @param type   the type
     * @param id     the id
     * @return the index response
     */
    public IndexResponse sendData(Map<String, ?> source, String index,
            String type, String id) {
        return indexQuery(buildIndexRequest(source, index, type, id));
    }

    /**
     * Send data string.
     *
     * @param source the source
     * @param index  the index
     * @param type   the type
     * @return the string
     */
    public String sendData(Map<String, ?> source, String index,
            String type) {
        return sendData(source, index, type, null).getId();
    }

    /**
     * Send data index response.
     *
     * @param jsonSource the json source
     * @param index      the index
     * @param type       the type
     * @param id         the id
     * @return the index response
     */
    public IndexResponse sendData(String jsonSource, String index, String type,
            String id) {
        return indexQuery(buildIndexRequest(jsonSource, index, type, id));
    }

    /**
     * Send data string.
     *
     * @param jsonSource the json source
     * @param index      the index
     * @param type       the type
     * @return the string
     */
    public String sendData(String jsonSource, String index, String type) {
        return sendData(jsonSource, index, type, null).getId();
    }

    /**
     * Send data with object mapper index response.
     *
     * @param sourceObject the source object
     * @param index        the index
     * @param type         the type
     * @param id           the id
     * @return the index response
     */
    public IndexResponse sendDataWithObjectMapper(Object sourceObject,
            String index, String type, String id) {
        return sendData(
                JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject),
                index, type, id);
    }

    /**
     * Send data with object mapper string.
     *
     * @param sourceObject the source object
     * @param index        the index
     * @param type         the type
     * @return the string
     */
    public String sendDataWithObjectMapper(Object sourceObject, String index,
            String type) {
        return sendDataWithObjectMapper(sourceObject, index, type, null)
                .getId();
    }

    /**
     * Send data async action future.
     *
     * @param source the source
     * @param index  the index
     * @param type   the type
     * @param id     the id
     * @return the action future
     */
    public ActionFuture<IndexResponse> sendDataAsync(
            Map<String, ?> source, String index, String type, String id) {
        return indexQueryAsync(buildIndexRequest(source, index, type, id));
    }

    /**
     * Send data async action future.
     *
     * @param source the source
     * @param index  the index
     * @param type   the type
     * @return the action future
     */
    public ActionFuture<IndexResponse> sendDataAsync(
            Map<String, ?> source, String index, String type) {
        return indexQueryAsync(buildIndexRequest(source, index, type, null));
    }

    /**
     * Send data async action future.
     *
     * @param jsonSource the json source
     * @param index      the index
     * @param type       the type
     * @param id         the id
     * @return the action future
     */
    public ActionFuture<IndexResponse> sendDataAsync(
            String jsonSource, String index, String type, String id) {
        return indexQueryAsync(buildIndexRequest(jsonSource, index, type, id));
    }

    /**
     * Send data async action future.
     *
     * @param jsonSource the json source
     * @param index      the index
     * @param type       the type
     * @return the action future
     */
    public ActionFuture<IndexResponse>
    sendDataAsync(String jsonSource, String index, String type) {
        return indexQueryAsync(
                buildIndexRequest(jsonSource, index, type, null));
    }

    /**
     * Send data async with object mapper action future.
     *
     * @param sourceObject the source object
     * @param index        the index
     * @param type         the type
     * @param id           the id
     * @return the action future
     */
    public ActionFuture<IndexResponse> sendDataAsyncWithObjectMapper(
            Object sourceObject, String index, String type, String id) {
        return indexQueryAsync(buildIndexRequest(
                JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject),
                index, type, id));
    }

    /**
     * Send data async with object mapper action future.
     *
     * @param sourceObject the source object
     * @param index        the index
     * @param type         the type
     * @return the action future
     */
    public ActionFuture<IndexResponse> sendDataAsyncWithObjectMapper(
            Object sourceObject, String index, String type) {
        return indexQueryAsync(buildIndexRequest(
                JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject),
                index, type, null));
    }

}
