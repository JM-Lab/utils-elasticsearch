package kr.jm.utils.elasticsearch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import kr.jm.utils.datastructure.JMCollections;
import kr.jm.utils.helper.JMOptional;
import kr.jm.utils.helper.JMThread;

/**
 * The Class JMElasticsearchClientTest.
 */
public class JMElasticsearchClientTest {

	static {
		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
	}

	private JMEmbeddedElastricsearch jmEmbeddedElasticsearch;

	private JMElasticsearchClient jmElasticsearchClient;

	/**
	 * Sets the up.
	 *
	 * @throws Exception
	 *             the exception
	 */
	@Before
	public void setUp() throws Exception {
		// Embedded Elasticsearch Node Start
		this.jmEmbeddedElasticsearch = new JMEmbeddedElastricsearch();
		this.jmEmbeddedElasticsearch.start();

		// JMElasticsearchClient Init
		this.jmElasticsearchClient = new JMElasticsearchClient(
				this.jmEmbeddedElasticsearch.getTransportIpPortPair());

		// Bulk Processor Setting
		int bulkActions = 3;
		long bulkSizeKB = 5 * 1024;
		int flushIntervalSeconds = 5;
		this.jmElasticsearchClient.setBulkProcessor(bulkActions, bulkSizeKB,
				flushIntervalSeconds);
	}

	/**
	 * Tear down.
	 *
	 * @throws Exception
	 *             the exception
	 */
	@After
	public void tearDown() throws Exception {
		JMOptional.getOptional(jmElasticsearchClient.getAllIndices())
				.ifPresent(indices -> jmElasticsearchClient.deleteIndices(
						indices.toArray(new String[indices.size()])));
		while (jmElasticsearchClient.getAllIndices().size() > 0)
			JMThread.sleep(1000);
		jmElasticsearchClient.close();
		jmEmbeddedElasticsearch.close();
	}

	/**
	 * Test get query all indices.
	 *
	 * @throws Exception
	 *             the exception
	 */
	@Test
	public void testGetQueryAllIndices() throws Exception {
		Map<String, Object> sourceObject = new HashMap<>();
		sourceObject.put("key", "test");
		String index = "client-test";
		String type = "testType";

		if (!jmElasticsearchClient.isExists(index))
			assertTrue(jmElasticsearchClient.create(index));
		System.out.println(jmElasticsearchClient
				.sendDataWithObjectMapper(sourceObject, index, type));
		JMThread.sleep(1000);
		assertEquals("[client-test]",
				jmElasticsearchClient.getAllIndices().toString());
	}

	/**
	 * Test.
	 */
	@Test
	public final void test() {
		Map<String, Object> sourceObject = new HashMap<>();
		sourceObject.put("key", "test");
		String index = "client-test";
		String type = "testType";
		String type2 = "testType2";

		if (!jmElasticsearchClient.isExists(index))
			assertTrue(jmElasticsearchClient.create(index));
		jmElasticsearchClient.sendDataWithObjectMapper(sourceObject, index,
				type);
		System.out.println(jmElasticsearchClient.count(index));
		System.out.println(jmElasticsearchClient.searchAll(index));
		String id = jmElasticsearchClient.sendDataWithObjectMapper(sourceObject,
				index, type2);
		System.out.println(id);
		String[] indices = { index };
		String[] types = { type, type2 };
		String[] types1 = { type2 };
		assertEquals(
				jmElasticsearchClient.searchAll(indices, types).getHits()
						.getHits().length,
				jmElasticsearchClient.searchAll(index).getHits()
						.getHits().length);
		System.out.println(jmElasticsearchClient.searchAll(indices, types1));

		jmElasticsearchClient.sendDataWithObjectMapper(sourceObject, index,
				type, id);
		System.out.println(jmElasticsearchClient.count(index));

		jmElasticsearchClient.sendDataWithObjectMapper(sourceObject, index,
				type2, id);
		System.out.println(jmElasticsearchClient.count(index));
		System.out.println(jmElasticsearchClient.count(indices, types1));

	}

	/**
	 * Test index status.
	 */
	@Test
	public final void testIndexStatus() {
		Map<String, Object> sourceObject = new HashMap<>();
		sourceObject.put("key", "test");
		String index = "test-2015.03.25";
		String[] indices = { index };
		String type = "test";

		if (!jmElasticsearchClient.isExists(index))
			assertTrue(jmElasticsearchClient.create(index));
		jmElasticsearchClient.sendData(sourceObject, index, type);
		JMThread.sleep(1000);
		ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings =
				jmElasticsearchClient.getMappingsResponse(indices);
		System.out.println(mappings);
		assertTrue(mappings.containsKey(index));
		assertTrue(mappings.get(index).containsKey(type));
		System.out.println(mappings.get(index).get(type).type());
		assertEquals(type, mappings.get(index).get(type).type());
	}

	/**
	 * Test search query all withstats aggr.
	 */
	@Test
	public final void testSearchQueryAllWithstatsAggr() {
		String index = "test-2015.05.12";
		String[] indices = { index };
		String type = "test-responsetime";
		String[] types = { type };
		String test30 = "test_30";
		String test400 = "test_400";
		String[] sumFields = { test30, test400 };

		Map<String, Object> sourceObject = new HashMap<>();
		String timestamp = "@timestamp";
		sourceObject.put(timestamp, "2015-05-12T00:59:00Z");
		sourceObject.put(test30, 30);
		sourceObject.put(test400, 400);

		Map<String, Object> sourceObject2 = new HashMap<>();
		sourceObject2.put(timestamp, "2015-05-12T00:59:02Z");
		sourceObject2.put(test30, 30);
		sourceObject2.put(test400, 400);

		Map<String, Object> sourceObject3 = new HashMap<>();
		sourceObject3.put(timestamp, "2015-05-12T01:00:00Z");
		sourceObject3.put(test30, 30);
		sourceObject3.put(test400, 400);

		if (!jmElasticsearchClient.isExists(index))
			assertTrue(jmElasticsearchClient.create(index));
		jmElasticsearchClient.sendData(sourceObject, index, type);
		jmElasticsearchClient.sendData(sourceObject2, index, type);
		jmElasticsearchClient.sendData(sourceObject3, index, type);
		// 인덱싱할 시간 필요
		JMThread.sleep(3000);

		SearchResponse searchResponse1 =
				jmElasticsearchClient.searchAll(indices, types);
		System.out.println(searchResponse1);

		AggregationBuilder[] sumAggregationBuilders =
				new AbstractAggregationBuilder[sumFields.length];
		for (int i = 0; i < sumFields.length; i++)
			sumAggregationBuilders[i] =
					AggregationBuilders.stats(sumFields[i]).field(sumFields[i]);
		searchResponse1 = jmElasticsearchClient.searchAll(indices, types,
				sumAggregationBuilders);
		System.out.println(searchResponse1);
		assertTrue(searchResponse1.toString().contains("\"count\":3"));
		assertTrue(searchResponse1.toString().contains("\"sum\":90.0"));
		QueryBuilder dataRangeFilter = QueryBuilders.rangeQuery(timestamp)
				.gte("2015-05-12T00:59:00Z").lt("2015-05-12T01:00:00Z");
		System.out.println(jmElasticsearchClient.searchAll(indices));
		searchResponse1 = jmElasticsearchClient.searchAll(indices, types,
				dataRangeFilter);
		assertTrue(searchResponse1.toString().contains("\"total\":2"));

		searchResponse1 = jmElasticsearchClient.searchAll(indices, types,
				dataRangeFilter, sumAggregationBuilders);
		assertTrue(searchResponse1.toString().contains("\"count\":2"));
		assertTrue(searchResponse1.toString().contains("\"sum\":60.0"));

	}

	/**
	 * Test get query filtered index list.
	 */
	@Test
	public final void testGetQueryFilteredIndexList() {
		Set<String> allIndexList = jmElasticsearchClient.getAllIndices();
		System.out.println(allIndexList);

		String containedString = "test";
		List<String> indexList =
				jmElasticsearchClient.getFilteredIndexList(containedString);
		System.out.println(indexList);
	}

	/**
	 * Test delete doc query indices.
	 */
	@Test
	public final void testDeleteDocQueryIndices() {
		Set<String> allIndexList = jmElasticsearchClient.getAllIndices();
		System.out.println(allIndexList);

		String containedString = "test-";

		List<String> indexList =
				jmElasticsearchClient.getFilteredIndexList(containedString);
		System.out.println(indexList);

		Map<String, Object> sourceObject = new HashMap<>();
		sourceObject.put("key", "test");
		String index = "test-2015.03.25";
		String type = "test";

		if (!jmElasticsearchClient.isExists(index))
			assertTrue(jmElasticsearchClient.create(index));
		jmElasticsearchClient.sendData(sourceObject, index, type);

		JMOptional.getOptional(jmElasticsearchClient.getAllIndices())
				.ifPresent(indices -> jmElasticsearchClient.deleteIndices(
						indices.toArray(new String[indices.size()])));
		JMThread.sleep(1000);
		allIndexList = jmElasticsearchClient.getAllIndices();
		System.out.println(allIndexList);
		assertEquals(0, indexList.size());
		assertEquals(0, allIndexList.size());

		indexList = jmElasticsearchClient.getFilteredIndexList(containedString);
		System.out.println(indexList);

	}

	/**
	 * Test send with bulk processor.
	 */
	@Test
	public void testSendWithBulkProcessor() {
		String index = "test-2015.05.12";
		String[] indices = { index };
		String type = "test-responsetime";
		String[] types = { type };
		String test30 = "test_30";
		String test400 = "test_400";
		String test500 = "test_500";

		Map<String, Object> sourceObject = new HashMap<>();
		String timestamp = "@timestamp";
		sourceObject.put(timestamp, "2015-05-12T00:59:00Z");
		sourceObject.put(test30, 30);
		sourceObject.put(test400, 400);

		Map<String, Object> sourceObject2 = new HashMap<>();
		sourceObject2.put(timestamp, "2015-05-12T00:59:02Z");
		sourceObject2.put(test30, 30);
		sourceObject2.put(test400, 400);

		Map<String, Object> sourceObject3 = new HashMap<>();
		sourceObject3.put(timestamp, "2015-05-12T01:00:00Z");
		sourceObject3.put(test30, 30);
		sourceObject3.put(test400, 400);
		sourceObject3.put(test500, 500);

		// hits 1건만 조회 되도록 설정
		jmElasticsearchClient.setDefaultHitsCount(1);

		jmElasticsearchClient.sendWithBulkProcessor(
				JMCollections.buildList(sourceObject, sourceObject2), index,
				type);

		Set<String> allIndexList = jmElasticsearchClient.getAllIndices();
		System.out.println(allIndexList);
		assertEquals("[]", allIndexList.toString());

		jmElasticsearchClient.sendWithBulkProcessor(JMCollections.buildList(
				sourceObject, sourceObject2, sourceObject3), index, type);
		// 인덱싱할 시간 필요
		JMThread.sleep(3000);
		allIndexList = jmElasticsearchClient.getAllIndices();
		System.out.println(allIndexList);
		assertEquals("[test-2015.05.12]", allIndexList.toString());
		SearchResponse searchResponse1 =
				jmElasticsearchClient.searchAll(indices, types);
		System.out.println(searchResponse1);
		assertFalse(searchResponse1.toString().contains(test500));

		JMThread.sleep(3000);
		searchResponse1 = jmElasticsearchClient.searchAll(indices, types);
		System.out.println(searchResponse1);
		assertEquals(1, searchResponse1.getHits().hits().length);

		// 기본 조회 건수에 영향 없이 count를 구해서 검색
		searchResponse1 =
				jmElasticsearchClient.searchAllWithTargetCount(indices, types);
		System.out.println(searchResponse1);
		assertTrue(searchResponse1.toString().contains(test500));
	}

	/**
	 * Test delete doc query.
	 *
	 * @throws Exception
	 *             the exception
	 */
	@Test
	public void testDeleteDocQuery() throws Exception {
		String index = "test-2015.05.12";
		String[] indices = { index };
		String type = "test-responsetime";
		String[] types = { type };
		String test30 = "test_30";
		String test400 = "test_400";

		Map<String, Object> sourceObject = new HashMap<>();
		String timestamp = "@timestamp";
		sourceObject.put(timestamp, "2015-05-12T00:59:00Z");
		sourceObject.put(test30, 30);
		sourceObject.put(test400, 400);

		Map<String, Object> sourceObject2 = new HashMap<>();
		sourceObject2.put(timestamp, "2015-05-12T00:59:02Z");
		sourceObject2.put(test30, 30);
		sourceObject2.put(test400, 400);

		Map<String, Object> sourceObject3 = new HashMap<>();
		sourceObject3.put(timestamp, "2015-05-12T01:00:00Z");
		sourceObject3.put(test30, 30);
		sourceObject3.put(test400, 400);

		if (!jmElasticsearchClient.isExists(index))
			assertTrue(jmElasticsearchClient.create(index));
		jmElasticsearchClient.sendData(sourceObject, index, type);
		jmElasticsearchClient.sendData(sourceObject2, index, type);
		jmElasticsearchClient.sendData(sourceObject3, index, type);
		// 인덱싱할 시간 필요
		JMThread.sleep(1000);

		SearchResponse searchResponse1 =
				jmElasticsearchClient.searchAll(indices, types);
		System.out.println(searchResponse1);

		JMThread.sleep(1000);
		searchResponse1 = jmElasticsearchClient.searchAll(indices, types);
		System.out.println(searchResponse1);

	}

	/**
	 * Test get query all id list.
	 *
	 * @throws Exception
	 *             the exception
	 */
	@Test
	public void testGetQueryAllIdList() throws Exception {
		String index = "test-2015.05.12";
		String type = "test-responsetime";
		String test30 = "test_30";
		String test400 = "test_400";
		String test500 = "test_500";

		Map<String, Object> sourceObject = new HashMap<>();
		String timestamp = "@timestamp";
		sourceObject.put(timestamp, "2015-05-12T00:59:00Z");
		sourceObject.put(test30, 30);
		sourceObject.put(test400, 400);

		Map<String, Object> sourceObject2 = new HashMap<>();
		sourceObject2.put(timestamp, "2015-05-12T00:59:02Z");
		sourceObject2.put(test30, 30);
		sourceObject2.put(test400, 400);

		Map<String, Object> sourceObject3 = new HashMap<>();
		sourceObject3.put(timestamp, "2015-05-12T01:00:00Z");
		sourceObject3.put(test30, 30);
		sourceObject3.put(test400, 400);
		sourceObject3.put(test500, 500);

		jmElasticsearchClient.sendWithBulkProcessor(JMCollections.buildList(
				sourceObject, sourceObject2, sourceObject3), index, type);
		JMThread.sleep(3000);
		Set<String> allIndexList = jmElasticsearchClient.getAllIndices();
		System.out.println(allIndexList);
		SearchResponse searchResponse1 =
				jmElasticsearchClient.searchAll(index, type);
		System.out.println(searchResponse1);
		List<String> allIdList =
				jmElasticsearchClient.getAllIdList(index, type);
		System.out.println(allIdList);
		Iterator<String> iterator = allIdList.iterator();
		assertTrue(Arrays.stream(searchResponse1.getHits().hits())
				.map(SearchHit::getId)
				.allMatch(id -> id.equals(iterator.next())));

	}

	/**
	 * Test delete doc bulk docs.
	 *
	 * @throws Exception
	 *             the exception
	 */
	@Test
	public void testDeleteDocBulkDocs() throws Exception {
		String index = "test-2015.05.12";
		String type = "test-responsetime";
		String test30 = "test_30";
		String test400 = "test_400";
		String test500 = "test_500";

		Map<String, Object> sourceObject = new HashMap<>();
		String timestamp = "@timestamp";
		sourceObject.put(timestamp, "2015-05-12T00:59:00Z");
		sourceObject.put(test30, 30);
		sourceObject.put(test400, 400);

		Map<String, Object> sourceObject2 = new HashMap<>();
		sourceObject2.put(timestamp, "2015-05-12T00:59:02Z");
		sourceObject2.put(test30, 30);
		sourceObject2.put(test400, 400);

		Map<String, Object> sourceObject3 = new HashMap<>();
		sourceObject3.put(timestamp, "2015-05-12T01:00:00Z");
		sourceObject3.put(test30, 30);
		sourceObject3.put(test400, 400);
		sourceObject3.put(test500, 500);

		jmElasticsearchClient.sendWithBulkProcessor(JMCollections.buildList(
				sourceObject, sourceObject2, sourceObject3), index, type);
		JMThread.sleep(3000);
		Set<String> allIndexList = jmElasticsearchClient.getAllIndices();
		System.out.println(allIndexList);
		SearchResponse searchResponse1 = jmElasticsearchClient.searchAll(index);
		System.out.println(searchResponse1);

		QueryBuilder filterQueryBuilder = QueryBuilders.rangeQuery(timestamp)
				.gte("2015-05-12T00:59:00Z").lt("2015-05-12T01:00:00Z");
		BulkResponse deleteDocs = jmElasticsearchClient.deleteBulkDocs(index,
				type, filterQueryBuilder);
		assertFalse(deleteDocs.hasFailures());

		JMThread.sleep(3000);
		searchResponse1 = jmElasticsearchClient.searchAll(index, type);
		System.out.println(searchResponse1);
		assertEquals(1, searchResponse1.getHits().hits().length);

		filterQueryBuilder = QueryBuilders.existsQuery(test500);
		jmElasticsearchClient.deleteBulkDocsAsync(index, type,
				filterQueryBuilder);
		JMThread.sleep(3000);

		searchResponse1 = jmElasticsearchClient.searchAll(index, type);
		System.out.println(searchResponse1);

		assertFalse(searchResponse1.toString().contains(test500));
		assertEquals(0, searchResponse1.getHits().hits().length);

	}

	/**
	 * Test delete doc bulk docs with multi.
	 *
	 * @throws Exception
	 *             the exception
	 */
	@Test
	public void testDeleteDocBulkDocsWithMulti() throws Exception {
		String index = "test-2015.05.12";
		String type = "test-responsetime";
		String test30 = "test_30";
		String test400 = "test_400";
		String test500 = "test_500";

		Map<String, Object> sourceObject = new HashMap<>();
		String timestamp = "@timestamp";
		sourceObject.put(timestamp, "2015-05-12T00:59:00Z");
		sourceObject.put(test30, 30);
		sourceObject.put(test400, 400);

		Map<String, Object> sourceObject2 = new HashMap<>();
		sourceObject2.put(timestamp, "2015-05-12T00:59:02Z");
		sourceObject2.put(test30, 30);
		sourceObject2.put(test400, 400);

		Map<String, Object> sourceObject3 = new HashMap<>();
		sourceObject3.put(timestamp, "2015-05-12T01:00:00Z");
		sourceObject3.put(test30, 30);
		sourceObject3.put(test400, 400);
		sourceObject3.put(test500, 500);

		jmElasticsearchClient.sendWithBulkProcessor(JMCollections.buildList(
				sourceObject, sourceObject2, sourceObject3), index, type);
		JMThread.sleep(3000);
		Set<String> allIndexList = jmElasticsearchClient.getAllIndices();
		System.out.println(allIndexList);
		SearchResponse searchResponse1 = jmElasticsearchClient.searchAll(index);
		System.out.println(searchResponse1);

		QueryBuilder filter = QueryBuilders.rangeQuery(timestamp)
				.gte("2015-05-12T00:59:00Z").lt("2015-05-12T01:00:00Z");
		boolean deleteDocs = jmElasticsearchClient.deleteBulkDocs(
				JMCollections.buildList(index), JMCollections.buildList(type),
				filter);
		assertTrue(deleteDocs);

		JMThread.sleep(3000);
		searchResponse1 = jmElasticsearchClient.searchAll(index, type);
		System.out.println(searchResponse1);
		assertEquals(1, searchResponse1.getHits().hits().length);

		filter = QueryBuilders.existsQuery(test500);
		jmElasticsearchClient.deleteBulkDocsAsync(
				JMCollections.buildList(index), JMCollections.buildList(type),
				filter);
		JMThread.sleep(3000);

		searchResponse1 = jmElasticsearchClient.searchAll(index, type);
		System.out.println(searchResponse1);

		assertFalse(searchResponse1.toString().contains(test500));
		assertEquals(0, searchResponse1.getHits().hits().length);

	}

	@Test
	public final void testUpsertQuery() {
		String index = "test-2015.05.12";
		String type = "test-responsetime";
		String test30 = "test_30";
		String test400 = "test_400";

		Map<String, Object> sourceObject = new HashMap<>();
		String timestamp = "@timestamp";
		sourceObject.put(timestamp, "2015-05-12T00:59:00Z");
		sourceObject.put(test30, 30);
		sourceObject.put(test400, 400);

		if (!jmElasticsearchClient.isExists(index))
			assertTrue(jmElasticsearchClient.create(index));
		jmElasticsearchClient.sendData(sourceObject, index, type, "1");
		// 인덱싱할 시간 필요
		JMThread.sleep(1000);
		SearchResponse searchResponse =
				jmElasticsearchClient.searchAll(index, type);
		System.out.println(searchResponse);
		SearchHits hits = searchResponse.getHits();
		SearchHit searchHit = hits.getAt(0);
		System.out.println(searchHit.getVersion());
		sourceObject = new HashMap<>();
		sourceObject.put(test400, 500);
		// 기존것에 다시 보내면 기존데이터를 새로운 것으로 변경 함
		jmElasticsearchClient.sendData(sourceObject, index, type, "1");
		JMThread.sleep(1000);
		searchResponse = jmElasticsearchClient.searchAll(index, type);
		System.out.println(searchResponse);
		hits = searchResponse.getHits();
		searchHit = hits.getAt(0);
		System.out.println(searchHit.getVersion());
		assertEquals(500, searchHit.getSource().get(test400));
		sourceObject = new HashMap<>();
		sourceObject.put("new", "newData");
		// upsertData 를 하면 기존 데이터를 그대로 둔 상태에서 새로 추가된 것만 넣거나 함
		jmElasticsearchClient.upsertData(sourceObject, index, type, "1");
		JMThread.sleep(1000);
		searchResponse = jmElasticsearchClient.searchAll(index, type);
		System.out.println(searchResponse);
		hits = searchResponse.getHits();
		searchHit = hits.getAt(0);
		// 버전 변경이 안됨 항상 -1
		System.out.println(searchHit.getVersion());
		assertEquals(500, searchHit.getSource().get(test400));
		assertEquals("newData", searchHit.getSource().get("new"));
	}

}
