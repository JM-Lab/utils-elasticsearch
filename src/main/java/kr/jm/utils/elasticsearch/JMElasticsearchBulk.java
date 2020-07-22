package kr.jm.utils.elasticsearch;

import kr.jm.utils.exception.JMExceptionManager;
import kr.jm.utils.helper.JMLog;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.bulk.BulkProcessor.Builder;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.*;
import static kr.jm.utils.helper.JMOptional.ifNotNull;
import static kr.jm.utils.helper.JMPredicate.peek;

/**
 * The type Jm elasticsearch bulk.
 */
@Slf4j
class JMElasticsearchBulk {

    private final JMElasticsearchClient jmESClient;
    private BulkProcessor bulkProcessor;
    private final ActionListener<BulkResponse> bulkResponseActionListener = new ActionListener<>() {
        @Override
        public void onResponse(BulkResponse bulkResponse) {
            if (bulkResponse.hasFailures()) {
                JMExceptionManager
                        .handleException(log, new RuntimeException("ElasticSearch Insert Bulk Error !!!"), "onResponse",
                                bulkResponse.buildFailureMessage());
            } else {
                logBulkSendingSuccess(bulkResponse);
            }
        }

        @Override
        public void onFailure(Exception e) {
            JMExceptionManager.handleException(log, e, "onFailure");
        }
    };

    private final Listener bulkProcessorListener = new Listener() {

        @Override
        public void beforeBulk(long executionId, BulkRequest bulkRequest) {
            log.debug("[Before] Sending Bulk - size = {}, estimatedSizeInBytes = {}", bulkRequest.requests().size(),
                    bulkRequest.estimatedSizeInBytes());
        }

        @Override
        public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable failure) {
            JMExceptionManager.handleException(log, failure, "afterBulk", executionId, bulkRequest.getDescription());
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse bulkResponse) {
            logBulkSendingSuccess(bulkResponse);
        }
    };

    /**
     * Instantiates a new Jm elasticsearch bulk.
     *
     * @param jmElasticsearchClient the jm elasticsearch client
     */
    JMElasticsearchBulk(JMElasticsearchClient jmElasticsearchClient) {
        this.jmESClient = jmElasticsearchClient;
    }

    private void logBulkSendingSuccess(BulkResponse bulkResponse) {
        log.debug("[Success] Sending Bulk - size = {}, tookInMillis = {}", bulkResponse.getItems().length,
                bulkResponse.getTook().millis());
    }

    private BulkProcessor setAndReturnBulkProcessor(BulkProcessor bulkProcessor) {
        return this.bulkProcessor = bulkProcessor;
    }

    /**
     * Sets bulk processor.
     *
     * @param bulkActions          the bulk actions
     * @param bulkSizeKB           the bulk size kb
     * @param flushIntervalSeconds the flush interval seconds
     */
    public void setBulkProcessor(int bulkActions, long bulkSizeKB, int flushIntervalSeconds) {
        setBulkProcessor(this.bulkProcessorListener, bulkActions, bulkSizeKB, flushIntervalSeconds);
    }

    /**
     * Sets bulk processor.
     *
     * @param bulkProcessorListener the bulk processor listener
     * @param bulkActions           the bulk actions
     * @param bulkSizeKB            the bulk size kb
     * @param flushIntervalSeconds  the flush interval seconds
     */
    public void setBulkProcessor(Listener bulkProcessorListener, int bulkActions, long bulkSizeKB,
            int flushIntervalSeconds) {
        this.bulkProcessor = buildBulkProcessor(bulkProcessorListener, bulkActions, bulkSizeKB, flushIntervalSeconds);
    }

    /**
     * Gets bulk processor builder.
     *
     * @param bulkProcessorListener the bulk processor listener
     * @param bulkActions           the bulk actions
     * @param byteSizeValue         the byte size value
     * @param flushInterval         the flush interval
     * @param concurrentRequests    the concurrent requests
     * @param backoffPolicy         the backoff policy
     * @return the bulk processor builder
     */
    public Builder getBulkProcessorBuilder(Listener bulkProcessorListener, Integer bulkActions,
            ByteSizeValue byteSizeValue, TimeValue flushInterval, Integer concurrentRequests,
            BackoffPolicy backoffPolicy) {
        Builder builder = getBuilder(bulkProcessorListener);
        ifNotNull(bulkActions, builder::setBulkActions);
        ifNotNull(byteSizeValue, builder::setBulkSize);
        ifNotNull(flushInterval, builder::setFlushInterval);
        ifNotNull(concurrentRequests, builder::setConcurrentRequests);
        ifNotNull(backoffPolicy, builder::setBackoffPolicy);
        return builder;
    }

    private Builder getBuilder(Listener bulkProcessorListener) {
        return BulkProcessor.builder(jmESClient, bulkProcessorListener);
    }

    /**
     * Build bulk processor bulk processor.
     *
     * @param bulkProcessorListener the bulk processor listener
     * @param bulkActions           the bulk actions
     * @param bulkSizeKB            the bulk size kb
     * @param flushIntervalSeconds  the flush interval seconds
     * @param concurrentRequests    the concurrent requests
     * @param backoffPolicy         the backoff policy
     * @return the bulk processor
     */
    public BulkProcessor buildBulkProcessor(Listener bulkProcessorListener, int bulkActions, long bulkSizeKB,
            int flushIntervalSeconds, Integer concurrentRequests, BackoffPolicy backoffPolicy) {
        return getBulkProcessorBuilder(bulkProcessorListener, bulkActions,
                new ByteSizeValue(bulkSizeKB, ByteSizeUnit.KB), TimeValue.timeValueSeconds(flushIntervalSeconds),
                concurrentRequests, backoffPolicy).build();
    }

    /**
     * Build bulk processor bulk processor.
     *
     * @param bulkProcessorListener the bulk processor listener
     * @param bulkActions           the bulk actions
     * @param bulkSizeKB            the bulk size kb
     * @param flushIntervalSeconds  the flush interval seconds
     * @return the bulk processor
     */
    public BulkProcessor buildBulkProcessor(Listener bulkProcessorListener, int bulkActions, long bulkSizeKB,
            int flushIntervalSeconds) {
        return buildBulkProcessor(bulkProcessorListener, bulkActions, bulkSizeKB, flushIntervalSeconds, null, null);
    }

    /**
     * Send with bulk processor.
     *
     * @param bulkSource the bulk source
     * @param index      the index
     */
    public void sendWithBulkProcessor(List<? extends Map<String, Object>> bulkSource, String index) {
        sendWithBulkProcessor(bulkSource.stream().map(source -> buildIndexRequest(index, null).source(source))
                .collect(toList()));
    }

    /**
     * Send with bulk processor.
     *
     * @param source the source
     * @param index  the index
     */
    public void sendWithBulkProcessor(Map<String, Object> source, String index) {
        sendWithBulkProcessor(source, index, null);
    }

    /**
     * Send with bulk processor.
     *
     * @param source the source
     * @param index  the index
     * @param id     the id
     */
    public void sendWithBulkProcessor(Map<String, Object> source, String index, String id) {
        sendWithBulkProcessor(buildIndexRequest(index, id).source(source));
    }

    /**
     * Send with bulk processor and object mapper.
     *
     * @param bulkObject the bulk object
     * @param index      the index
     */
    public void sendWithBulkProcessorAndObjectMapper(List<Object> bulkObject, String index) {
        sendWithBulkProcessor(bulkObject.stream().map(sourceObject -> buildIndexRequest(index, null)
                .source(JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject))).collect(toList()));
    }

    private IndexRequest buildIndexRequest(String index, String id) {
        return id == null ? new IndexRequest(index) : new IndexRequest(index).id(id);
    }

    /**
     * Send with bulk processor and object mapper.
     *
     * @param object the object
     * @param index  the index
     */
    public void sendWithBulkProcessorAndObjectMapper(Object object, String index) {
        sendWithBulkProcessorAndObjectMapper(object, index, null);
    }

    /**
     * Send with bulk processor and object mapper.
     *
     * @param object the object
     * @param index  the index
     * @param id     the id
     */
    public void sendWithBulkProcessorAndObjectMapper(Object object, String index, String id) {
        sendWithBulkProcessor(
                buildIndexRequest(index, id).source(JMElasticsearchUtil.buildSourceByJsonMapper(object)));
    }

    /**
     * Send with bulk processor.
     *
     * @param indexRequestList the index request list
     */
    public void sendWithBulkProcessor(List<IndexRequest> indexRequestList) {
        indexRequestList.forEach(this::sendWithBulkProcessor);
    }

    /**
     * Send with bulk processor.
     *
     * @param indexRequest the index request
     */
    public void sendWithBulkProcessor(IndexRequest indexRequest) {
        Optional.ofNullable(this.bulkProcessor).orElseGet(
                () -> setAndReturnBulkProcessor(getBuilder(bulkProcessorListener).build()))
                .add(indexRequest);
    }

    /**
     * Close bulk processor.
     */
    public void closeBulkProcessor() {
        Optional.ofNullable(bulkProcessor).filter(peek(BulkProcessor::flush)).ifPresent(BulkProcessor::close);
    }

    /**
     * Send bulk data async.
     *
     * @param bulkSourceList the bulk source list
     * @param index          the index
     */
    public void sendBulkDataAsync(List<? extends Map<String, Object>> bulkSourceList, String index) {
        executeBulkRequestAsync(buildBulkIndexRequestBuilder(
                bulkSourceList.stream().map(source -> jmESClient.prepareIndex().setIndex(index).setSource(source))
                        .collect(toList())));
    }

    /**
     * Send bulk data async.
     *
     * @param bulkSourceList             the bulk source list
     * @param index                      the index
     * @param bulkResponseActionListener the bulk response action listener
     */
    public void sendBulkDataAsync(List<? extends Map<String, Object>> bulkSourceList, String index,
            ActionListener<BulkResponse> bulkResponseActionListener) {
        executeBulkRequestAsync(buildBulkIndexRequestBuilder(
                bulkSourceList.stream().map(source -> jmESClient.prepareIndex().setIndex(index).setSource(source))
                        .collect(toList())), bulkResponseActionListener);
    }

    /**
     * Send bulk data with object mapper async.
     *
     * @param objectBulkData the object bulk data
     * @param index          the index
     */
    public void sendBulkDataWithObjectMapperAsync(List<Object> objectBulkData, String index) {
        executeBulkRequestAsync(buildBulkIndexRequestBuilder(objectBulkData.stream()
                .map(sourceObject -> jmESClient.prepareIndex().setIndex(index)
                        .setSource(JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject))).collect(toList())));
    }

    /**
     * Send bulk data with object mapper async.
     *
     * @param objectBulkData             the object bulk data
     * @param index                      the index
     * @param bulkResponseActionListener the bulk response action listener
     */
    public void sendBulkDataWithObjectMapperAsync(List<Object> objectBulkData, String index,
            ActionListener<BulkResponse> bulkResponseActionListener) {
        executeBulkRequestAsync(buildBulkIndexRequestBuilder(objectBulkData.stream()
                        .map(sourceObject -> jmESClient.prepareIndex().setIndex(index)
                                .setSource(JMElasticsearchUtil.buildSourceByJsonMapper(sourceObject))).collect(toList())),
                bulkResponseActionListener);
    }

    /**
     * Build bulk index request builder bulk request builder.
     *
     * @param indexRequestBuilderList the index request builder list
     * @return the bulk request builder
     */
    public BulkRequestBuilder buildBulkIndexRequestBuilder(List<IndexRequestBuilder> indexRequestBuilderList) {
        BulkRequestBuilder bulkRequestBuilder = jmESClient.prepareBulk();
        for (IndexRequestBuilder indexRequestBuilder : indexRequestBuilderList)
            bulkRequestBuilder.add(indexRequestBuilder);
        return bulkRequestBuilder;
    }

    /**
     * Build delete bulk request builder bulk request builder.
     *
     * @param deleteRequestBuilderList the delete request builder list
     * @return the bulk request builder
     */
    public BulkRequestBuilder buildDeleteBulkRequestBuilder(List<DeleteRequestBuilder> deleteRequestBuilderList) {
        BulkRequestBuilder bulkRequestBuilder = jmESClient.prepareBulk();
        for (DeleteRequestBuilder deleteRequestBuilder : deleteRequestBuilderList)
            bulkRequestBuilder.add(deleteRequestBuilder);
        return bulkRequestBuilder;
    }

    /**
     * Build update bulk request builder bulk request builder.
     *
     * @param updateRequestBuilderList the update request builder list
     * @return the bulk request builder
     */
    public BulkRequestBuilder buildUpdateBulkRequestBuilder(List<UpdateRequestBuilder> updateRequestBuilderList) {
        BulkRequestBuilder bulkRequestBuilder = jmESClient.prepareBulk();
        for (UpdateRequestBuilder updateRequestBuilder : updateRequestBuilderList)
            bulkRequestBuilder.add(updateRequestBuilder);
        return bulkRequestBuilder;
    }

    /**
     * Execute bulk request async.
     *
     * @param bulkRequestBuilder the bulk request builder
     */
    public void executeBulkRequestAsync(BulkRequestBuilder bulkRequestBuilder) {
        executeBulkRequestAsync(bulkRequestBuilder, bulkResponseActionListener);
    }

    /**
     * Execute bulk request async.
     *
     * @param bulkRequestBuilder         the bulk request builder
     * @param bulkResponseActionListener the bulk response action listener
     */
    public void executeBulkRequestAsync(BulkRequestBuilder bulkRequestBuilder,
            ActionListener<BulkResponse> bulkResponseActionListener) {
        JMLog.info(log, "executeBulkRequestAsync", bulkRequestBuilder, bulkResponseActionListener);
        bulkRequestBuilder.execute(bulkResponseActionListener);
    }

    /**
     * Execute bulk request bulk response.
     *
     * @param bulkRequestBuilder the bulk request builder
     * @return the bulk response
     */
    public BulkResponse executeBulkRequest(BulkRequestBuilder bulkRequestBuilder) {
        JMLog.info(log, "executeBulkRequest", bulkRequestBuilder);
        return bulkRequestBuilder.execute().actionGet();
    }

    /**
     * Delete bulk docs boolean.
     *
     * @param index the index
     * @return the boolean
     */
    public boolean deleteBulkDocs(String index) {
        return executeBulkRequest(buildDeleteBulkRequestBuilder(buildAllDeleteRequestBuilderList(index)))
                .hasFailures();
    }

    /**
     * Delete bulk docs bulk response.
     *
     * @param index              the index
     * @param filterQueryBuilder the filter query builder
     * @return the bulk response
     */
    public BulkResponse deleteBulkDocs(String index, QueryBuilder filterQueryBuilder) {
        return executeBulkRequest(
                buildDeleteBulkRequestBuilder(buildExtractDeleteRequestBuilderList(index, filterQueryBuilder)));
    }

    /**
     * Delete bulk docs boolean.
     *
     * @param indexList          the index list
     * @param filterQueryBuilder the filter query builder
     * @return the boolean
     */
    public boolean deleteBulkDocs(List<String> indexList, QueryBuilder filterQueryBuilder) {
        return indexList.stream().map(index -> deleteBulkDocs(index, filterQueryBuilder))
                .noneMatch(BulkResponse::hasFailures);
    }

    /**
     * Delete bulk docs async.
     *
     * @param index the index
     */
    public void deleteBulkDocsAsync(String index) {
        executeBulkRequestAsync(buildDeleteBulkRequestBuilder(buildAllDeleteRequestBuilderList(index)));
    }

    /**
     * Delete bulk docs async.
     *
     * @param index                      the index
     * @param bulkResponseActionListener the bulk response action listener
     */
    public void deleteBulkDocsAsync(String index, ActionListener<BulkResponse> bulkResponseActionListener) {
        executeBulkRequestAsync(buildDeleteBulkRequestBuilder(buildAllDeleteRequestBuilderList(index)),
                bulkResponseActionListener);
    }

    /**
     * Delete bulk docs async.
     *
     * @param index              the index
     * @param filterQueryBuilder the filter query builder
     */
    public void deleteBulkDocsAsync(String index, QueryBuilder filterQueryBuilder) {
        executeBulkRequestAsync(
                buildDeleteBulkRequestBuilder(buildExtractDeleteRequestBuilderList(index, filterQueryBuilder)));
    }

    /**
     * Delete bulk docs async.
     *
     * @param index                      the index
     * @param filterQueryBuilder         the filter query builder
     * @param bulkResponseActionListener the bulk response action listener
     */
    public void deleteBulkDocsAsync(String index, QueryBuilder filterQueryBuilder,
            ActionListener<BulkResponse> bulkResponseActionListener) {
        executeBulkRequestAsync(
                buildDeleteBulkRequestBuilder(buildExtractDeleteRequestBuilderList(index, filterQueryBuilder)),
                bulkResponseActionListener);
    }

    /**
     * Delete bulk docs async.
     *
     * @param indexList                  the index list
     * @param filterQueryBuilder         the filter query builder
     * @param bulkResponseActionListener the bulk response action listener
     */
    public void deleteBulkDocsAsync(List<String> indexList, QueryBuilder filterQueryBuilder,
            ActionListener<BulkResponse> bulkResponseActionListener) {
        indexList.forEach(index -> deleteBulkDocsAsync(index, filterQueryBuilder, bulkResponseActionListener));
    }

    private List<DeleteRequestBuilder> buildAllDeleteRequestBuilderList(String index) {
        return buildDeleteRequestBuilderList(index, jmESClient.getAllIdList(index));
    }

    private List<DeleteRequestBuilder> buildExtractDeleteRequestBuilderList(String index,
            QueryBuilder filterQueryBuilder) {
        return buildDeleteRequestBuilderList(index, jmESClient.extractIdList(index, filterQueryBuilder));
    }

    private List<DeleteRequestBuilder> buildDeleteRequestBuilderList(String index, List<String> idList) {
        return idList.stream().map(id -> jmESClient.prepareDelete().setIndex(index).setId(id)).collect(toList());
    }

}
