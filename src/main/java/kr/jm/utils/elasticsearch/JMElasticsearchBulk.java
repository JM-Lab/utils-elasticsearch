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

	public void setBulkProcessor(Listener bulkProcessorListener,
			int bulkActions, ByteSizeValue bulkSize, TimeValue flushInterval,
			int concurrentRequests) {
		this.bulkProcessor = buildBulkProcessor(bulkProcessorListener,
				bulkActions, bulkSize, flushInterval, concurrentRequests);
	}

	public void setBulkProcessor(int bulkActions, ByteSizeValue bulkSize,
			TimeValue flushInterval, int concurrentRequests) {
		this.bulkProcessor = buildBulkProcessor(this.bulkProcessorListener,
				bulkActions, bulkSize, flushInterval, concurrentRequests);
	}

	public BulkProcessor buildBulkProcessor(Listener bulkProcessorListener,
			int bulkActions, ByteSizeValue bulkSize, TimeValue flushInterval,
			int concurrentRequests) {
		return BulkProcessor.builder(jmESClient, bulkProcessorListener)
				.setBulkActions(bulkActions).setBulkSize(bulkSize)
				.setFlushInterval(flushInterval)
				.setConcurrentRequests(concurrentRequests).build();
	}

	public void sendBulkDataAsync(List<Map<String, Object>> bulkSourceList,
			String index, String type) {
		excuteBulkRequestAsync(
				buildBulkIndexRequestBuilder(bulkSourceList
						.stream().map(source -> jmESClient
								.prepareIndex(index, type).setSource(source))
				.collect(toList())));
	}

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

	public void sendBulkDataWithObjectMapperAsync(List<Object> objectBulkData,
			String index, String type) {
		excuteBulkRequestAsync(buildBulkIndexRequestBuilder(objectBulkData
				.stream()
				.map(sourceObject -> jmESClient.prepareIndex(index, type)
						.setSource(JMElastricsearchUtil
								.buildSourceByJsonMapper(sourceObject)))
				.collect(toList())));
	}

	public void sendBulkDataWithObjectMapperAsync(List<Object> objectBulkData,
			String index, String type,
			ActionListener<BulkResponse> bulkResponseActionListener) {
		excuteBulkRequestAsync(buildBulkIndexRequestBuilder(objectBulkData
				.stream()
				.map(sourceObject -> jmESClient.prepareIndex(index, type)
						.setSource(JMElastricsearchUtil
								.buildSourceByJsonMapper(sourceObject)))
				.collect(toList())), bulkResponseActionListener);
	}

	public BulkRequestBuilder buildBulkIndexRequestBuilder(
			List<IndexRequestBuilder> indexRequestBuilderList) {
		return Optional.of(jmESClient.prepareBulk())
				.filter(peek(bulkRequestBuilder -> indexRequestBuilderList
						.forEach(bulkRequestBuilder::add)))
				.get();
	}

	public BulkRequestBuilder buildDeleteBulkRequestBuilder(
			List<DeleteRequestBuilder> deleteRequestBuilderList) {
		return Optional.of(jmESClient.prepareBulk())
				.filter(peek(bulkRequestBuilder -> deleteRequestBuilderList
						.forEach(bulkRequestBuilder::add)))
				.get();
	}

	public BulkRequestBuilder buildUpdateBulkRequestBuilder(
			List<IndexRequestBuilder> indexRequestBuilderList) {
		return Optional.of(jmESClient.prepareBulk())
				.filter(peek(bulkRequestBuilder -> indexRequestBuilderList
						.forEach(bulkRequestBuilder::add)))
				.get();
	}

	public void excuteBulkRequestAsync(BulkRequestBuilder bulkRequestBuilder) {
		bulkRequestBuilder.execute(bulkResponseActionListener);
	}

	public void excuteBulkRequestAsync(BulkRequestBuilder bulkRequestBuilder,
			ActionListener<BulkResponse> bulkResponseActionListener) {
		bulkRequestBuilder.execute(bulkResponseActionListener);
	}

	public BulkResponse
			excuteBulkRequest(BulkRequestBuilder bulkRequestBuilder) {
		return bulkRequestBuilder.execute().actionGet();
	}

	public void sendWithBulkProcessor(List<Map<String, Object>> bulkSource,
			String index, String type) {
		sendWithBulkProcessor(bulkSource.stream()
				.map(source -> new IndexRequest(index, type).source(source))
				.collect(toList()));
	}

	public void sendWithBulkProcessorAndObjectMapper(List<Object> bulkObject,
			String index, String type) {
		sendWithBulkProcessor(bulkObject.stream()
				.map(sourceObject -> new IndexRequest(index, type)
						.source(JMElastricsearchUtil
								.buildSourceByJsonMapper(sourceObject)))
				.collect(toList()));
	}

	public void sendWithBulkProcessor(List<IndexRequest> indexRequestList) {
		BulkProcessor bulkProcessor = Optional.ofNullable(this.bulkProcessor)
				.orElseGet(() -> setAndReturnBulkProcessor(BulkProcessor
						.builder(jmESClient, bulkProcessorListener).build()));
		indexRequestList.forEach(bulkProcessor::add);
	}

	public void closeBulkProcessor() {
		Optional.ofNullable(bulkProcessor).filter(peek(BulkProcessor::flush))
				.ifPresent(BulkProcessor::close);
	}

	public boolean deleteBulkDocs(String index, String type) {
		return excuteBulkRequest(
				buildDeleteBulkRequestBuilder(
						jmESClient.getAllIdList(index, type)
								.stream().map(id -> jmESClient
										.prepareDelete(index, type, id))
				.collect(toList()))).hasFailures();
	}

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

	public void deleteBulkDocsAsync(String index, String type) {
		excuteBulkRequestAsync(
				buildDeleteBulkRequestBuilder(
						jmESClient.getAllIdList(index, type)
								.stream().map(id -> jmESClient
										.prepareDelete(index, type, id))
				.collect(toList())));
	}

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

	public void deleteBulkDocsAsync(String index, String type,
			FilterBuilder filterBuilder,
			ActionListener<BulkResponse> bulkResponseActionListener) {
		excuteBulkRequestAsync(
				buildDeleteBulkRequestBuilder(jmESClient
						.extractIdList(jmESClient.searchAll(index, type,
								filterBuilder))
						.stream()
						.map(id -> jmESClient.prepareDelete(index, type, id))
						.collect(toList())),
				bulkResponseActionListener);
	}

}
