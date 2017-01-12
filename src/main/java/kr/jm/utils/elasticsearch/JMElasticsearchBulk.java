package kr.jm.utils.elasticsearch;

import static java.util.stream.Collectors.toList;
import static kr.jm.utils.helper.JMOptional.ifNotNull;
import static kr.jm.utils.helper.JMPredicate.peek;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Builder;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;

import kr.jm.utils.exception.JMExceptionManager;
import kr.jm.utils.helper.JMLog;
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
				public void onFailure(Exception e) {
					JMExceptionManager.logException(log, e, "onFailure");
				}
			};

	private Listener bulkProcessorListener = new Listener() {

		@Override
		public void beforeBulk(long executionId, BulkRequest bulkRequest) {
			log.info(
					"[Before] Sending Bulk - size = {}, estimatedSizeInBytes = {}",
					bulkRequest.requests().size(),
					bulkRequest.estimatedSizeInBytes());
		}

		@Override
		public void afterBulk(long executionId, BulkRequest bulkRequest,
				Throwable failure) {
			JMExceptionManager.logException(log, failure, "afterBulk",
					executionId, bulkRequest.getDescription());
		}

		@Override
		public void afterBulk(long executionId, BulkRequest request,
				BulkResponse bulkResponse) {
			logBulkSendingSuccess(bulkResponse);
		}
	};

	private void logBulkSendingSuccess(BulkResponse bulkResponse) {
		log.info("[Success] Sending Bulk - size = {}, tookInMillis = {}",
				bulkResponse.getItems().length, bulkResponse.getTookInMillis());
	}

	private BulkProcessor
			setAndReturnBulkProcessor(BulkProcessor bulkProcessor) {
		return this.bulkProcessor = bulkProcessor;
	}

	/**
	 * Sets the bulk processor.
	 *
	 * @param bulkActions
	 *            the bulk actions
	 * @param bulkSizeKB
	 *            the bulk size KB
	 * @param flushIntervalSeconds
	 *            the flush interval seconds
	 */
	public void setBulkProcessor(int bulkActions, long bulkSizeKB,
			int flushIntervalSeconds) {
		setBulkProcessor(this.bulkProcessorListener, bulkActions, bulkSizeKB,
				flushIntervalSeconds);
	}

	/**
	 * Sets the bulk processor.
	 *
	 * @param bulkProcessorListener
	 *            the bulk processor listener
	 * @param bulkActions
	 *            the bulk actions
	 * @param bulkSizeKB
	 *            the bulk size KB
	 * @param flushIntervalSeconds
	 *            the flush interval seconds
	 */
	public void setBulkProcessor(Listener bulkProcessorListener,
			int bulkActions, long bulkSizeKB, int flushIntervalSeconds) {
		this.bulkProcessor = buildBulkProcessor(bulkProcessorListener,
				bulkActions, bulkSizeKB, flushIntervalSeconds);
	}

	/**
	 * Gets the bulk processor builder.
	 *
	 * @param bulkProcessorListener
	 *            the bulk processor listener
	 * @param bulkActions
	 *            the bulk actions
	 * @param byteSizeValue
	 *            the byte size value
	 * @param flushInterval
	 *            the flush interval
	 * @param concurrentRequests
	 *            the concurrent requests
	 * @param backoffPolicy
	 *            the backoff policy
	 * @return the bulk processor builder
	 */
	public Builder getBulkProcessorBuilder(Listener bulkProcessorListener,
			Integer bulkActions, ByteSizeValue byteSizeValue,
			TimeValue flushInterval, Integer concurrentRequests,
			BackoffPolicy backoffPolicy) {
		Builder builder =
				BulkProcessor.builder(jmESClient, bulkProcessorListener);
		ifNotNull(bulkActions, builder::setBulkActions);
		ifNotNull(byteSizeValue, builder::setBulkSize);
		ifNotNull(flushInterval, builder::setFlushInterval);
		ifNotNull(concurrentRequests, builder::setConcurrentRequests);
		ifNotNull(backoffPolicy, builder::setBackoffPolicy);
		return builder;
	}

	/**
	 * Builds the bulk processor.
	 *
	 * @param bulkProcessorListener
	 *            the bulk processor listener
	 * @param bulkActions
	 *            the bulk actions
	 * @param bulkSizeKB
	 *            the bulk size KB
	 * @param flushIntervalSeconds
	 *            the flush interval seconds
	 * @param concurrentRequests
	 *            the concurrent requests
	 * @param backoffPolicy
	 *            the backoff policy
	 * @return the bulk processor
	 */
	public BulkProcessor buildBulkProcessor(Listener bulkProcessorListener,
			int bulkActions, long bulkSizeKB, int flushIntervalSeconds,
			Integer concurrentRequests, BackoffPolicy backoffPolicy) {
		return getBulkProcessorBuilder(bulkProcessorListener, bulkActions,
				new ByteSizeValue(bulkSizeKB, ByteSizeUnit.KB),
				TimeValue.timeValueSeconds(flushIntervalSeconds),
				concurrentRequests, backoffPolicy).build();
	}

	/**
	 * Builds the bulk processor.
	 *
	 * @param bulkProcessorListener
	 *            the bulk processor listener
	 * @param bulkActions
	 *            the bulk actions
	 * @param bulkSizeKB
	 *            the bulk size KB
	 * @param flushIntervalSeconds
	 *            the flush interval seconds
	 * @return the bulk processor
	 */
	public BulkProcessor buildBulkProcessor(Listener bulkProcessorListener,
			int bulkActions, long bulkSizeKB, int flushIntervalSeconds) {
		return buildBulkProcessor(bulkProcessorListener, bulkActions,
				bulkSizeKB, flushIntervalSeconds, null, null);
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

	public void sendWithBulkProcessor(Map<String, Object> source, String index,
			String type) {
		sendWithBulkProcessor(new IndexRequest(index, type).source(source));
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

	public void sendWithBulkProcessorAndObjectMapper(Object object,
			String index, String type) {
		sendWithBulkProcessor(new IndexRequest(index, type)
				.source(JMElastricsearchUtil.buildSourceByJsonMapper(object)));
	}

	/**
	 * Send with bulk processor.
	 *
	 * @param indexRequestList
	 *            the index request list
	 */
	public void sendWithBulkProcessor(List<IndexRequest> indexRequestList) {
		indexRequestList.forEach(this::sendWithBulkProcessor);
	}

	public void sendWithBulkProcessor(IndexRequest indexRequest) {
		Optional.ofNullable(this.bulkProcessor)
				.orElseGet(() -> setAndReturnBulkProcessor(BulkProcessor
						.builder(jmESClient, bulkProcessorListener).build()))
				.add(indexRequest);
	}

	/**
	 * Close bulk processor.
	 */
	public void closeBulkProcessor() {
		Optional.ofNullable(bulkProcessor).filter(peek(BulkProcessor::flush))
				.ifPresent(BulkProcessor::close);
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
		excuteBulkRequestAsync(bulkRequestBuilder, bulkResponseActionListener);
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
		JMLog.info(log, "excuteBulkRequestAsync", bulkRequestBuilder,
				bulkResponseActionListener);
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
		JMLog.info(log, "excuteBulkRequest", bulkRequestBuilder);
		return bulkRequestBuilder.execute().actionGet();
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
		return excuteBulkRequest(buildDeleteBulkRequestBuilder(
				buildAllDeleteRequestBuilderList(index, type))).hasFailures();
	}

	/**
	 * Delete bulk docs.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @return the bulk response
	 */
	public BulkResponse deleteBulkDocs(String index, String type,
			QueryBuilder filterQueryBuilder) {
		return excuteBulkRequest(buildDeleteBulkRequestBuilder(
				buildExtractDeleteRequestBuilderList(index, type,
						filterQueryBuilder)));
	}

	/**
	 * Delete bulk docs.
	 *
	 * @param indexList
	 *            the index list
	 * @param typeList
	 *            the type list
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @return true, if successful
	 */
	public boolean deleteBulkDocs(List<String> indexList, List<String> typeList,
			QueryBuilder filterQueryBuilder) {
		return indexList.stream().flatMap(index -> typeList.stream()
				.map(type -> deleteBulkDocs(index, type, filterQueryBuilder)))
				.noneMatch(reponse -> reponse.hasFailures());
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
		excuteBulkRequestAsync(buildDeleteBulkRequestBuilder(
				buildAllDeleteRequestBuilderList(index, type)));
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
						buildAllDeleteRequestBuilderList(index, type)),
				bulkResponseActionListener);
	}

	/**
	 * Delete bulk docs async.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param filterQueryBuilder
	 *            the filter query builder
	 */
	public void deleteBulkDocsAsync(String index, String type,
			QueryBuilder filterQueryBuilder) {
		excuteBulkRequestAsync(buildDeleteBulkRequestBuilder(
				buildExtractDeleteRequestBuilderList(index, type,
						filterQueryBuilder)));
	}

	/**
	 * Delete bulk docs async.
	 *
	 * @param indexList
	 *            the index list
	 * @param typeList
	 *            the type list
	 * @param filterQueryBuilder
	 *            the filter query builder
	 */
	public void deleteBulkDocsAsync(List<String> indexList,
			List<String> typeList, QueryBuilder filterQueryBuilder) {
		indexList.forEach(index -> typeList.forEach(
				type -> deleteBulkDocsAsync(index, type, filterQueryBuilder)));
	}

	/**
	 * Delete bulk docs async.
	 *
	 * @param index
	 *            the index
	 * @param type
	 *            the type
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @param bulkResponseActionListener
	 *            the bulk response action listener
	 */
	public void deleteBulkDocsAsync(String index, String type,
			QueryBuilder filterQueryBuilder,
			ActionListener<BulkResponse> bulkResponseActionListener) {
		excuteBulkRequestAsync(
				buildDeleteBulkRequestBuilder(
						buildExtractDeleteRequestBuilderList(index, type,
								filterQueryBuilder)),
				bulkResponseActionListener);
	}

	/**
	 * Delete bulk docs async.
	 *
	 * @param indexList
	 *            the index list
	 * @param typeList
	 *            the type list
	 * @param filterQueryBuilder
	 *            the filter query builder
	 * @param bulkResponseActionListener
	 *            the bulk response action listener
	 */
	public void deleteBulkDocsAsync(List<String> indexList,
			List<String> typeList, QueryBuilder filterQueryBuilder,
			ActionListener<BulkResponse> bulkResponseActionListener) {
		indexList.forEach(
				index -> typeList.forEach(type -> deleteBulkDocsAsync(index,
						type, filterQueryBuilder, bulkResponseActionListener)));
	}

	private List<DeleteRequestBuilder>
			buildAllDeleteRequestBuilderList(String index, String type) {
		return buildDeleteRequestBuilderList(index, type,
				jmESClient.getAllIdList(index, type));
	}

	private List<DeleteRequestBuilder> buildExtractDeleteRequestBuilderList(
			String index, String type, QueryBuilder filterQueryBuilder) {
		return buildDeleteRequestBuilderList(index, type,
				jmESClient.extractIdList(index, type, filterQueryBuilder));
	}

	private List<DeleteRequestBuilder> buildDeleteRequestBuilderList(
			String index, String type, List<String> idList) {
		return idList.stream()
				.map(id -> jmESClient.prepareDelete(index, type, id))
				.collect(toList());
	}

}
