package kr.jm.utils.elasticsearch;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.Client;

/**
 * The type Jm elasticsearch delete.
 */
public class JMElasticsearchDelete {

	private Client esClient;

    /**
     * Instantiates a new Jm elasticsearch delete.
     *
     * @param elasticsearchClient the elasticsearch client
     */
    public JMElasticsearchDelete(Client elasticsearchClient) {
		this.esClient = elasticsearchClient;
	}

    /**
     * Delete query delete response.
     *
     * @param deleteRequestBuilder the delete request builder
     * @return the delete response
     */
    public DeleteResponse
	deleteQuery(DeleteRequestBuilder deleteRequestBuilder) {
		return JMElasticsearchUtil.logRequestQueryAndReturn("deleteQuery",
				deleteRequestBuilder, deleteRequestBuilder.execute());
	}

    /**
     * Delete indices delete index response.
     *
     * @param indices the indices
     * @return the delete index response
     */
    public DeleteIndexResponse deleteIndices(String... indices) {
		DeleteIndexRequestBuilder requestBuilder =
				esClient.admin().indices().prepareDelete(indices);
		return JMElasticsearchUtil.logRequestQueryAndReturn("deleteIndices",
				requestBuilder, requestBuilder.execute());
	}

    /**
     * Delete doc delete response.
     *
     * @param index the index
     * @param type  the type
     * @param id    the id
     * @return the delete response
     */
    public DeleteResponse deleteDoc(String index, String type, String id) {
		return deleteQuery(esClient.prepareDelete(index, type, id));
	}

}
