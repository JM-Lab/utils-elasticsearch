package kr.jm.utils.elasticsearch;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * The Class JMElasticsearchDelete.
 */
public class JMElasticsearchDelete {

	private Client esClient;

	/**
	 * Instantiates a new JM elasticsearch delete.
	 *
	 * @param elasticsearchClient
	 *            the elasticsearch client
	 */
	public JMElasticsearchDelete(Client elasticsearchClient) {
		this.esClient = elasticsearchClient;
	}

	/**
	 * Delete query.
	 *
	 * @param deleteRequestBuilder
	 *            the delete request builder
	 * @return the delete response
	 */
	public DeleteResponse
			deleteQuery(DeleteRequestBuilder deleteRequestBuilder) {
		return JMElastricsearchUtil.logExcuteAndReturn("deleteQuery",
				deleteRequestBuilder, deleteRequestBuilder.execute());
	}

	/**
	 * Delete indices.
	 *
	 * @param indices
	 *            the indices
	 * @return the delete index response
	 */
	public DeleteIndexResponse deleteIndices(String... indices) {
		DeleteIndexRequestBuilder requestBuilder =
				esClient.admin().indices().prepareDelete(indices);
		return JMElastricsearchUtil.logExcuteAndReturn("deleteIndices",
				requestBuilder, requestBuilder.execute());
	}

	/**
	 * Delete doc.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param id
	 *            the id
	 * @return the delete response
	 */
	public DeleteResponse deleteDoc(String index, String type, String id) {
		return deleteQuery(esClient.prepareDelete(index, type, id));
	}

	/**
	 * Delete docs.
	 *
	 * @param deleteByQueryRequestBuilder
	 *            the delete by query request builder
	 * @return the delete by query response
	 */
	// ver 2 대 deprecated 됨 deleteByQuery plugin으로 제공 함, 현재는 제공되니 그냥 사용
	@Deprecated
	public DeleteByQueryResponse deleteDocs(
			DeleteByQueryRequestBuilder deleteByQueryRequestBuilder) {
		return JMElastricsearchUtil.logExcuteAndReturn("deleteDocs",
				deleteByQueryRequestBuilder,
				deleteByQueryRequestBuilder.execute());
	}

	/**
	 * Delete all docs.
	 *
	 * @param indices
	 *            the indices
	 * @return the delete by query response
	 */
	@Deprecated
	public DeleteByQueryResponse deleteAllDocs(String... indices) {
		return deleteDocs(QueryBuilders.matchAllQuery(), indices);
	}

	/**
	 * Delete docs.
	 *
	 * @param queryBuilder
	 *            the query builder
	 * @param indices
	 *            the indices
	 * @return the delete by query response
	 */
	@Deprecated
	public DeleteByQueryResponse deleteDocs(QueryBuilder queryBuilder,
			String... indices) {
		return deleteDocs(
				esClient.prepareDeleteByQuery(indices).setQuery(queryBuilder));

	}

	/**
	 * Delete docs.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param queryBuilder
	 *            the query builder
	 * @return the delete by query response
	 */
	@Deprecated
	public DeleteByQueryResponse deleteDocs(String[] indices, String[] types,
			QueryBuilder queryBuilder) {
		return deleteDocs(esClient.prepareDeleteByQuery(indices).setTypes(types)
				.setQuery(queryBuilder));
	}

	/**
	 * Delete docs.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @param filterBuilder
	 *            the filter builder
	 * @return the delete by query response
	 */
	@Deprecated
	public DeleteByQueryResponse deleteDocs(String[] indices, String[] types,
			FilterBuilder filterBuilder) {
		return deleteDocs(indices, types, QueryBuilders
				.filteredQuery(QueryBuilders.matchAllQuery(), filterBuilder));
	}

	/**
	 * Delete all docs.
	 *
	 * @param indices
	 *            the indices
	 * @param types
	 *            the types
	 * @return the delete by query response
	 */
	@Deprecated
	public DeleteByQueryResponse deleteAllDocs(String[] indices,
			String[] types) {
		return deleteDocs(esClient.prepareDeleteByQuery(indices)
				.setQuery(QueryBuilders.matchAllQuery()).setTypes(types));
	}

}
