package kr.jm.utils.elasticsearch;

import java.util.Map;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;

/**
 * The Class JMElasticsearchIndex.
 */
public class JMElasticsearchIndex {

	private Client jmESClient;

	/**
	 * Instantiates a new JM elasticsearch index.
	 *
	 * @param elasticsearchClient
	 *            the elasticsearch client
	 */
	public JMElasticsearchIndex(Client elasticsearchClient) {
		this.jmESClient = elasticsearchClient;
	}

	/**
	 * Index query.
	 *
	 * @param indexRequestBuilder
	 *            the index request builder
	 * @return the index response
	 */
	public IndexResponse indexQuery(IndexRequestBuilder indexRequestBuilder) {
		return JMElastricsearchUtil.logExcuteAndReturn("indexQuery",
				indexRequestBuilder, indexRequestBuilder.execute());
	}

	/**
	 * Index query async.
	 *
	 * @param indexRequestBuilder
	 *            the index request builder
	 * @return the listenable action future
	 */
	public ListenableActionFuture<IndexResponse>
			indexQueryAsync(IndexRequestBuilder indexRequestBuilder) {
		return JMElastricsearchUtil.logExcuteAndReturnAsync("indexQueryAsync",
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

	/**
	 * Send data.
	 *
	 * @param source
	 *            the source
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param id
	 *            the id
	 * @return the index response
	 */
	public IndexResponse sendData(Map<String, Object> source, String index,
			String type, String id) {
		return indexQuery(buildIndexRequest(source, index, type, id));
	}

	/**
	 * Send data.
	 *
	 * @param source
	 *            the source
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @return the string
	 */
	public String sendData(Map<String, Object> source, String index,
			String type) {
		return sendData(source, index, type, null).getId();
	}

	/**
	 * Send data.
	 *
	 * @param jsonSource
	 *            the json source
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param id
	 *            the id
	 * @return the index response
	 */
	public IndexResponse sendData(String jsonSource, String index, String type,
			String id) {
		return indexQuery(buildIndexRequest(jsonSource, index, type, id));
	}

	/**
	 * Send data.
	 *
	 * @param jsonSource
	 *            the json source
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @return the string
	 */
	public String sendData(String jsonSource, String index, String type) {
		return sendData(jsonSource, index, type, null).getId();
	}

	/**
	 * Send data with object mapper.
	 *
	 * @param sourceObject
	 *            the source object
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param id
	 *            the id
	 * @return the index response
	 */
	public IndexResponse sendDataWithObjectMapper(Object sourceObject,
			String index, String type, String id) {
		return sendData(
				JMElastricsearchUtil.buildSourceByJsonMapper(sourceObject),
				index, type, id);
	}

	/**
	 * Send data with object mapper.
	 *
	 * @param sourceObject
	 *            the source object
	 * @param index
	 *            the index
	 * @param type
	 *            the type
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
	 * @param source
	 *            the source
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param id
	 *            the id
	 * @return the listenable action future
	 */
	public ListenableActionFuture<IndexResponse> sendDataAsync(
			Map<String, Object> source, String index, String type, String id) {
		return indexQueryAsync(buildIndexRequest(source, index, type, id));
	}

	/**
	 * Send data async.
	 *
	 * @param jsonSource
	 *            the json source
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param id
	 *            the id
	 * @return the listenable action future
	 */
	public ListenableActionFuture<IndexResponse> sendDataAsync(
			String jsonSource, String index, String type, String id) {
		return indexQueryAsync(buildIndexRequest(jsonSource, index, type, id));
	}

	/**
	 * Send data async.
	 *
	 * @param sourceObject
	 *            the source object
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param id
	 *            the id
	 * @return the listenable action future
	 */
	public ListenableActionFuture<IndexResponse> sendDataAsync(
			Object sourceObject, String index, String type, String id) {
		return indexQueryAsync(buildIndexRequest(
				JMElastricsearchUtil.buildSourceByJsonMapper(sourceObject),
				index, type, id));
	}

}
