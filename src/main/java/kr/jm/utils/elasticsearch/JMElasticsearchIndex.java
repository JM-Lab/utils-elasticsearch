package kr.jm.utils.elasticsearch;

import java.util.Map;

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
		return indexQuery(
				jmESClient.prepareIndex(index, type, id).setSource(jsonSource));
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
		return indexQuery(
				jmESClient.prepareIndex(index, type, id).setSource(source));
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
		return indexQuery(
				jmESClient.prepareIndex(index, type).setSource(jsonSource))
						.getId();
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
		return indexQuery(
				jmESClient.prepareIndex(index, type).setSource(source)).getId();
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
		return indexQuery(jmESClient.prepareIndex(index, type).setSource(
				JMElastricsearchUtil.buildSourceByJsonMapper(sourceObject)))
						.getId();
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
		return indexQuery(jmESClient.prepareIndex(index, type, id).setSource(
				JMElastricsearchUtil.buildSourceByJsonMapper(sourceObject)));
	}

}
