package kr.jm.utils.elasticsearch;

import java.util.Map;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;

public class JMElasticsearchIndex {

	private Client jmESClient;

	public JMElasticsearchIndex(Client elasticsearchClient) {
		this.jmESClient = elasticsearchClient;
	}

	public IndexResponse indexQuery(IndexRequestBuilder indexRequestBuilder) {
		return JMElastricsearchUtil.logExcuteAndReturn("indexQuery",
				indexRequestBuilder, indexRequestBuilder.execute());
	}

	public IndexResponse sendData(String jsonSource, String index, String type,
			String id) {
		return indexQuery(
				jmESClient.prepareIndex(index, type, id).setSource(jsonSource));
	}

	public IndexResponse sendData(Map<String, Object> source, String index,
			String type, String id) {
		return indexQuery(
				jmESClient.prepareIndex(index, type, id).setSource(source));
	}

	public String sendData(String jsonSource, String index, String type) {
		return indexQuery(
				jmESClient.prepareIndex(index, type).setSource(jsonSource))
						.getId();
	}

	public String sendData(Map<String, Object> source, String index,
			String type) {
		return indexQuery(
				jmESClient.prepareIndex(index, type).setSource(source)).getId();
	}

	public String sendDataWithObjectMapper(Object sourceObject, String index,
			String type) {
		return indexQuery(jmESClient.prepareIndex(index, type).setSource(
				JMElastricsearchUtil.buildSourceByJsonMapper(sourceObject)))
						.getId();
	}

	public IndexResponse sendDataWithObjectMapper(Object sourceObject,
			String index, String type, String id) {
		return indexQuery(jmESClient.prepareIndex(index, type, id).setSource(
				JMElastricsearchUtil.buildSourceByJsonMapper(sourceObject)));
	}

}
