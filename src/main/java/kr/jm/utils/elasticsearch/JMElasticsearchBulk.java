package kr.jm.utils.elasticsearch;

import static java.util.stream.Collectors.toList;
import static kr.jm.utils.helper.JMPredicate.peek;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;

import kr.jm.utils.exception.JMExceptionManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class JMElasticsearchBulk {

	private JMElasticsearchClient jmESClient;
	private BulkProcessor bulkProcessor;

	JMElasticsearchBulk(JMElasticsearchClient jmElasticsearchClient) {
		this.jmESClient = jmElasticsearchClient;
	}

	private ActionListener<BulkResponse> bulkResponseActionListener =
			new ActionListener<BulkResponse>() {
				@Override
				public void onResponse(BulkResponse bulkResponse) {
					if (bulkResponse.hasFailures()) {
						JMExceptionManager.logException(log,
								new RuntimeException(
										"ElasticSearch Insert Bulk Error !!!"),
								"onResponse",
								bulkResponse.buildFailureMessage());
					} else {
						logBulkSendingSuccess(bulkResponse);
					}
				}

				@Override
				public void onFailure(Throwable e) {
					JMExceptionManager.logException(log, e, "onFailure");
				}
			};

	private Listener bulkProcessorListener = new Listener() {

		@Override
		public void beforeBulk(long executionId, BulkRequest bulkRequest) {
			log.info("send to ES - {} size Bulk sending bytes - {}",
					bulkRequest.requests().size(),
					bulkRequest.estimatedSizeInBytes());
		}

		@Override
		public void afterBulk(long executionId, BulkRequest bulkRequest,
				Throwable failure) {
			JMExceptionManager.logException(log, failure, "afterBulk",
					executionId, bulkRequest.getHeaders());
		}

		@Override
		public void afterBulk(long executionId, BulkRequest request,
				BulkResponse bulkResponse) {
			logBulkSendingSuccess(bulkResponse);
		}
	};

	private void logBulkSendingSuccess(BulkResponse bulkResponse) {
		log.info("send to ES - {} size Bulk sending time [{}m]",
				bulkResponse.getItems().length, bulkResponse.getTookInMillis());
	}

	private BulkProcessor
			setAndReturnBulkProcessor(BulkProcessor bulkProcessor) {
		return this.bulkProcessor = bulkProcessor;
	}

	/**
	 * Sets the bulk processor.
	 *
	 * @param bulkProcessorListener
	 *            the bulk processor listener
	 * @param bulkActions
	 *            the bulk actions
	 * @param bulkSize
	 *            the bulk size
	 * @param flushInterval
	 *            the flush interval
	 * @param concurrentRequests
	 *            the concurrent requests
	 */
	public void setBulkProcessor(Listener bulkProcessorListener,
			int bulkActions, ByteSizeValue bulkSize, TimeValue flushInterval,
			int concurrentRequests) {
		this.bulkProcessor = buildBulkProcessor(bulkProcessorListener,
				bulkActions, bulkSize, flushInterval, concurrentRequests);
	}

	/**
	 * Sets the bulk processor.
	 *
	 * @param bulkActions
	 *            the bulk actions
	 * @param bulkSize
	 *            the bulk size
	 * @param flushInterval
	 *            the flush interval
	 * @param concurrentRequests
	 *            the concurrent requests
	 */
	public void setBulkProcessor(int bulkActions, ByteSizeValue bulkSize,
			TimeValue flushInterval, int concurrentRequests) {
		this.bulkProcessor = buildBulkProcessor(this.bulkProcessorListener,
				bulkActions, bulkSize, flushInterval, concurrentRequests);
	}

	/**
	 * Builds the bulk processor.
	 *
	 * @param bulkProcessorListener
	 *            the bulk processor listener
	 * @param bulkActions
	 *            the bulk actions
	 * @param bulkSize
	 *            the bulk size
	 * @param flushInterval
	 *            the flush interval
	 * @param concurrentRequests
	 *            the concurrent requests
	 * @return the bulk processor
	 */
	public BulkProcessor buildBulkProcessor(Listener bulkProcessorListener,
			int bulkActions, ByteSizeValue bulkSize, TimeValue flushInterval,
			int concurrentRequests) {
		return BulkProcessor.builder(jmESClient, bulkProcessorListener)
				.setBulkActions(bulkActions).setBulkSize(bulkSize)
				.setFlushInterval(flushInterval)
				.setConcurrentRequests(concurrentRequests).build();
	}

	/**
	 * Send bulk data async.
	 *
	 * @param bulkSourceList
	 *            the bulk source list
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 */
	public void sendBulkDataAsync(List<Map<String, Object>> bulkSourceList,
			String index, String type) {
		excuteBulkRequestAsync(
				buildBulkIndexRequestBuilder(bulkSourceList
						.stream().map(source -> jmESClient
								.prepareIndex(index, type).setSource(source))
						.collect(toList())));
	}

	/**
	 * Send bulk data async.
	 *
	 * @param bulkSourceList
	 *            the bulk source list
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param bulkResponseActionListener
	 *            the bulk response action listener
	 */
	public void sendBulkDataAsync(List<Map<String, Object>> bulkSourceList,
			String index, String type,
			ActionListener<BulkResponse> bulkResponseActionListener) {
		excuteBulkRequestAsync(
				buildBulkIndexRequestBuilder(bulkSourceList.stream()
						.map(source -> jmESClient.prepareIndex(index, type)
								.setSource(source))
						.collect(toList())),
				bulkResponseActionListener);
	}

	/**
	 * Send bulk data with object mapper async.
	 *
	 * @param objectBulkData
	 *            the object bulk data
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 */
	public void sendBulkDataWithObjectMapperAsync(List<Object> objectBulkData,
			String index, String type) {
		excuteBulkRequestAsync(buildBulkIndexRequestBuilder(objectBulkData
				.stream()
				.map(sourceObject -> jmESClient.prepareIndex(index, type)
						.setSource(JMElastricsearchUtil
								.buildSourceByJsonMapper(sourceObject)))
				.collect(toList())));
	}

	/**
	 * Send bulk data with object mapper async.
	 *
	 * @param objectBulkData
	 *            the object bulk data
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param bulkResponseActionListener
	 *            the bulk response action listener
	 */
	public void sendBulkDataWithObjectMapperAsync(List<Object> objectBulkData,
			String index, String type,
			ActionListener<BulkResponse> bulkResponseActionListener) {
		excuteBulkRequestAsync(
				buildBulkIndexRequestBuilder(objectBulkData.stream()
						.map(sourceObject -> jmESClient
								.prepareIndex(index, type)
								.setSource(JMElastricsearchUtil
										.buildSourceByJsonMapper(sourceObject)))
						.collect(toList())),
				bulkResponseActionListener);
	}

	/**
	 * Builds the bulk index request builder.
	 *
	 * @param indexRequestBuilderList
	 *            the index request builder list
	 * @return the bulk request builder
	 */
	public BulkRequestBuilder buildBulkIndexRequestBuilder(
			List<IndexRequestBuilder> indexRequestBuilderList) {
		return Optional.of(jmESClient.prepareBulk())
				.filter(peek(bulkRequestBuilder -> indexRequestBuilderList
						.forEach(bulkRequestBuilder::add)))
				.get();
	}

	/**
	 * Builds the delete bulk request builder.
	 *
	 * @param deleteRequestBuilderList
	 *            the delete request builder list
	 * @return the bulk request builder
	 */
	public BulkRequestBuilder buildDeleteBulkRequestBuilder(
			List<DeleteRequestBuilder> deleteRequestBuilderList) {
		return Optional.of(jmESClient.prepareBulk())
				.filter(peek(bulkRequestBuilder -> deleteRequestBuilderList
						.forEach(bulkRequestBuilder::add)))
				.get();
	}

	/**
	 * Builds the update bulk request builder.
	 *
	 * @param indexRequestBuilderList
	 *            the index request builder list
	 * @return the bulk request builder
	 */
	public BulkRequestBuilder buildUpdateBulkRequestBuilder(
			List<IndexRequestBuilder> indexRequestBuilderList) {
		return Optional.of(jmESClient.prepareBulk())
				.filter(peek(bulkRequestBuilder -> indexRequestBuilderList
						.forEach(bulkRequestBuilder::add)))
				.get();
	}

	/**
	 * Excute bulk request async.
	 *
	 * @param bulkRequestBuilder
	 *            the bulk request builder
	 */
	public void excuteBulkRequestAsync(BulkRequestBuilder bulkRequestBuilder) {
		bulkRequestBuilder.execute(bulkResponseActionListener);
	}

	/**
	 * Excute bulk request async.
	 *
	 * @param bulkRequestBuilder
	 *            the bulk request builder
	 * @param bulkResponseActionListener
	 *            the bulk response action listener
	 */
	public void excuteBulkRequestAsync(BulkRequestBuilder bulkRequestBuilder,
			ActionListener<BulkResponse> bulkResponseActionListener) {
		bulkRequestBuilder.execute(bulkResponseActionListener);
	}

	/**
	 * Excute bulk request.
	 *
	 * @param bulkRequestBuilder
	 *            the bulk request builder
	 * @return the bulk response
	 */
	public BulkResponse
			excuteBulkRequest(BulkRequestBuilder bulkRequestBuilder) {
		return bulkRequestBuilder.execute().actionGet();
	}

	/**
	 * Send with bulk processor.
	 *
	 * @param bulkSource
	 *            the bulk source
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 */
	public void sendWithBulkProcessor(List<Map<String, Object>> bulkSource,
			String index, String type) {
		sendWithBulkProcessor(bulkSource.stream()
				.map(source -> new IndexRequest(index, type).source(source))
				.collect(toList()));
	}

	/**
	 * Send with bulk processor and object mapper.
	 *
	 * @param bulkObject
	 *            the bulk object
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 */
	public void sendWithBulkProcessorAndObjectMapper(List<Object> bulkObject,
			String index, String type) {
		sendWithBulkProcessor(bulkObject.stream()
				.map(sourceObject -> new IndexRequest(index, type)
						.source(JMElastricsearchUtil
								.buildSourceByJsonMapper(sourceObject)))
				.collect(toList()));
	}

	/**
	 * Send with bulk processor.
	 *
	 * @param indexRequestList
	 *            the index request list
	 */
	public void sendWithBulkProcessor(List<IndexRequest> indexRequestList) {
		BulkProcessor bulkProcessor = Optional.ofNullable(this.bulkProcessor)
				.orElseGet(() -> setAndReturnBulkProcessor(BulkProcessor
						.builder(jmESClient, bulkProcessorListener).build()));
		indexRequestList.forEach(bulkProcessor::add);
	}

	/**
	 * Close bulk processor.
	 */
	public void closeBulkProcessor() {
		Optional.ofNullable(bulkProcessor).filter(peek(BulkProcessor::flush))
				.ifPresent(BulkProcessor::close);
	}

	/**
	 * Delete bulk docs.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @return true, if successful
	 */
	public boolean deleteBulkDocs(String index, String type) {
		return excuteBulkRequest(
				buildDeleteBulkRequestBuilder(
						jmESClient.getAllIdList(index, type)
								.stream().map(id -> jmESClient
										.prepareDelete(index, type, id))
								.collect(toList()))).hasFailures();
	}

	/**
	 * Delete bulk docs.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param filterBuilder
	 *            the filter builder
	 * @return the bulk response
	 */
	public BulkResponse deleteBulkDocs(String index, String type,
			FilterBuilder filterBuilder) {
		return excuteBulkRequest(
				buildDeleteBulkRequestBuilder(jmESClient
						.extractIdList(jmESClient.searchAll(index, type,
								filterBuilder))
						.stream()
						.map(id -> jmESClient.prepareDelete(index, type, id))
						.collect(toList())));
	}

	/**
	 * Delete bulk docs async.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 */
	public void deleteBulkDocsAsync(String index, String type) {
		excuteBulkRequestAsync(
				buildDeleteBulkRequestBuilder(
						jmESClient.getAllIdList(index, type)
								.stream().map(id -> jmESClient
										.prepareDelete(index, type, id))
								.collect(toList())));
	}

	/**
	 * Delete bulk docs async.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param bulkResponseActionListener
	 *            the bulk response action listener
	 */
	public void deleteBulkDocsAsync(String index, String type,
			ActionListener<BulkResponse> bulkResponseActionListener) {
		excuteBulkRequestAsync(
				buildDeleteBulkRequestBuilder(
						jmESClient.getAllIdList(index, type).stream()
								.map(id -> jmESClient.prepareDelete(index, type,
										id))
								.collect(toList())),
				bulkResponseActionListener);
	}

	/**
	 * Delete bulk docs async.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param filterBuilder
	 *            the filter builder
	 */
	public void deleteBulkDocsAsync(String index, String type,
			FilterBuilder filterBuilder) {
		excuteBulkRequestAsync(
				buildDeleteBulkRequestBuilder(jmESClient
						.extractIdList(jmESClient.searchAll(index, type,
								filterBuilder))
						.stream()
						.map(id -> jmESClient.prepareDelete(index, type, id))
						.collect(toList())));
	}

	/**
	 * Delete bulk docs async.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param filterBuilder
	 *            the filter builder
	 * @param bulkResponseActionListener
	 *            the bulk response action listener
	 */
	public void deleteBulkDocsAsync(String index, String type,
			FilterBuilder filterBuilder,
			ActionListener<BulkResponse> bulkResponseActionListener) {
		excuteBulkRequestAsync(
				buildDeleteBulkRequestBuilder(
						jmESClient
								.extractIdList(jmESClient.searchAll(index, type,
										filterBuilder))
								.stream()
								.map(id -> jmESClient.prepareDelete(index, type,
										id))
								.collect(toList())),
				bulkResponseActionListener);
	}

}
