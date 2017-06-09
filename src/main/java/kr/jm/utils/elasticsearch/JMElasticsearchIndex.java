package kr.jm.utils.elasticsearch;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;

import java.util.Map;

/**
 * The Class JMElasticsearchIndex.
 */
public class JMElasticsearchIndex {

	private Client jmESClient;

	/**
	 * Instantiates a new JM elasticsearch index.
	 *
	 * @param elasticsearchClient the elasticsearch client
	 */
	public JMElasticsearchIndex(Client elasticsearchClient) {
		this.jmESClient = elasticsearchClient;
	}

	/**
	 * Index query.
	 *
	 * @param indexRequestBuilder the index request builder
	 * @return the index response
	 */
	public IndexResponse indexQuery(IndexRequestBuilder indexRequestBuilder) {
		return JMElasticsearchUtil.logExecuteAndReturn("indexQuery",
				indexRequestBuilder, indexRequestBuilder.execute());
	}

	/**
	 * Index query async.
	 *
	 * @param indexRequestBuilder the index request builder
	 * @return the listenable action future
	 */
	public ListenableActionFuture<IndexResponse>
	indexQueryAsync(IndexRequestBuilder indexRequestBuilder) {
		return JMElasticsearchUtil.logExecuteAndReturnAsync("indexQueryAsync",
				indexRequestBuilder, indexRequestBuilder.execute());
	}

	private IndexRequestBuilder buildIndexRequest(String jsonSource,
			String index, String type, String id) {
		return getPrepareIndex(index, type, id).setSource(jsonSource);
	}

	private IndexRequestBuilder buildIndexRequest(Map<String, Object> source,
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
			String id, Map<String, Object> source) {
		return jmESClient.prepareUpdate(index, type, id).setDoc(source)
				.setUpsert(new IndexRequest(index, type, id).source(source));
	}

	public UpdateResponse
	upsertQuery(UpdateRequestBuilder updateRequestBuilder) {
		return JMElasticsearchUtil.logExecuteAndReturn("upsertQuery",
				updateRequestBuilder, updateRequestBuilder.execute());
	}

	public ListenableActionFuture<UpdateResponse>
	upsertQueryAsync(UpdateRequestBuilder updateRequestBuilder) {
		return JMElasticsearchUtil.logExecuteAndReturnAsync("upsertQueryAsync",
				updateRequestBuilder, updateRequestBuilder.execute());
	}

	public UpdateResponse upsertData(Map<String, Object> source, String index,
			String type, String id) {
		return upsertQuery(buildPrepareUpsert(index, type, id, source));
	}

	public ListenableActionFuture<UpdateResponse> upsertDataAsync(
			Map<String, Object> source, String index, String type, String id) {
		return upsertQueryAsync(buildPrepareUpsert(index, type, id, source));
	}

	public UpdateResponse upsertData(String jsonSource, String index,
			String type, String id) {
		return upsertQuery(buildPrepareUpsert(index, type, id, jsonSource));
	}

	public ListenableActionFuture<UpdateResponse> upsertDataAsync(
			String jsonSource, String index, String type, String id) {
		return upsertQueryAsync(
				buildPrepareUpsert(index, type, id, jsonSource));
	}

	public UpdateResponse upsertDataWithObjectMapper(Object sourceObject,
			String index, String type, String id) {
		return upsertData(
				JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject),
				index, type, id);
	}

	public ListenableActionFuture<UpdateResponse>
	upsertDataASyncWithObjectMapper(Object sourceObject, String index,
			String type, String id) {
		return upsertDataAsync(
				JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject),
				index, type, id);
	}

	/**
	 * Send data.
	 *
	 * @param source the source
	 * @param index  the index
	 * @param type   the type
	 * @param id     the id
	 * @return the index response
	 */
	public IndexResponse sendData(Map<String, Object> source, String index,
			String type, String id) {
		return indexQuery(buildIndexRequest(source, index, type, id));
	}

	/**
	 * Send data.
	 *
	 * @param source the source
	 * @param index  the index
	 * @param type   the type
	 * @return the string
	 */
	public String sendData(Map<String, Object> source, String index,
			String type) {
		return sendData(source, index, type, null).getId();
	}

	/**
	 * Send data.
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
	 * Send data.
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
	 * Send data with object mapper.
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
	 * Send data with object mapper.
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
	 * Send data async.
	 *
	 * @param source the source
	 * @param index  the index
	 * @param type   the type
	 * @param id     the id
	 * @return the listenable action future
	 */
	public ListenableActionFuture<IndexResponse> sendDataAsync(
			Map<String, Object> source, String index, String type, String id) {
		return indexQueryAsync(buildIndexRequest(source, index, type, id));
	}

	public ListenableActionFuture<IndexResponse> sendDataAsync(
			Map<String, Object> source, String index, String type) {
		return indexQueryAsync(buildIndexRequest(source, index, type, null));
	}

	/**
	 * Send data async.
	 *
	 * @param jsonSource the json source
	 * @param index      the index
	 * @param type       the type
	 * @param id         the id
	 * @return the listenable action future
	 */
	public ListenableActionFuture<IndexResponse> sendDataAsync(
			String jsonSource, String index, String type, String id) {
		return indexQueryAsync(buildIndexRequest(jsonSource, index, type, id));
	}

	public ListenableActionFuture<IndexResponse>
	sendDataAsync(String jsonSource, String index, String type) {
		return indexQueryAsync(
				buildIndexRequest(jsonSource, index, type, null));
	}

	/**
	 * Send data async.
	 *
	 * @param sourceObject the source object
	 * @param index        the index
	 * @param type         the type
	 * @param id           the id
	 * @return the listenable action future
	 */
	public ListenableActionFuture<IndexResponse> sendDataAsyncWithObjectMapper(
			Object sourceObject, String index, String type, String id) {
		return indexQueryAsync(buildIndexRequest(
				JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject),
				index, type, id));
	}

	public ListenableActionFuture<IndexResponse> sendDataAsyncWithObjectMapper(
			Object sourceObject, String index, String type) {
		return indexQueryAsync(buildIndexRequest(
				JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject),
				index, type, null));
	}

}
