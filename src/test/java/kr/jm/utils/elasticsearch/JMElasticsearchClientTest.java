package kr.jm.utils.elasticsearch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import kr.jm.utils.helper.JMThread;

public class JMElasticsearchClientTest {

	static {
		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
	}

	private Node elasticsearch;

	private JMElasticsearchClient jmElasticsearchClient;

	int defaultPageSize = 5;
	int retryCount = 2;
	int timeOutSeconds = 3;

	@Before
	public void setUp() throws Exception {
		this.elasticsearch = NodeBuilder.nodeBuilder().build();
		this.elasticsearch.start();
		// nodeClient init
		// this.jmElasticsearchClient =
		// new JMElasticsearchClient(false, "localhost:9300,127.0.0.1:9300",
		// false);

		// transportClient init
		this.jmElasticsearchClient = new JMElasticsearchClient();
		System.out.println(jmElasticsearchClient.getSettings().getAsMap());
	}

	@After
	public void tearDown() throws Exception {
		Set<String> allIndices = jmElasticsearchClient.getAllIndices();
		jmElasticsearchClient.deleteIndices(
				allIndices.toArray(new String[allIndices.size()]));
		JMThread.sleep(1000);
		this.elasticsearch.stop();
	}

	@Test
	public void testGetAllIndices() throws Exception {
		Map<String, Object> sourceObject = new HashMap<String, Object>();
		sourceObject.put("key", "test");
		String index = "client-test";
		String type = "testType";

		if (!jmElasticsearchClient.isExists(index))
			assertTrue(jmElasticsearchClient.create(index));
		System.out.println(jmElasticsearchClient
				.sendDataWithObjectMapper(sourceObject, index, type));

		assertEquals("[client-test]",
				jmElasticsearchClient.getAllIndices().toString());
	}

	@Test
	public final void test() {
		Map<String, Object> sourceObject = new HashMap<String, Object>();
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
		String id = jmElasticsearchClient.sendData(sourceObject, index, type2);
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

	@Test
	public final void testIndexStatus() {
		Map<String, Object> sourceObject = new HashMap<String, Object>();
		sourceObject.put("key", "test");
		String index = "host-2015.03.25";
		String[] indices = { index };
		String type = "healthcheck_rspnstime";

		if (!jmElasticsearchClient.isExists(index))
			assertTrue(jmElasticsearchClient.create(index));
		jmElasticsearchClient.sendData(sourceObject, index, type);

		GetMappingsResponse getMappingsResponse =
				jmElasticsearchClient.getMappings(indices);
		ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings =
				getMappingsResponse.getMappings();
		System.out.println(mappings);
		assertTrue(mappings.toString()
				.contains("host-2015.03.25=>[healthcheck_rspnstime=>"));
		System.out.println(mappings.get(index).get(type).type());
		assertEquals(type, mappings.get(index).get(type).type());
	}

	@Test
	public final void testSearchAllWithstatsAggr() {
		String index = "test-2015.05.12";
		String[] indices = { index };
		String type = "test-responsetime";
		String[] types = { type };
		String test30 = "test_30";
		String test400 = "test_400";
		String[] sumFields = { test30, test400 };

		Map<String, Object> sourceObject = new HashMap<String, Object>();
		String timestamp = "@timestamp";
		sourceObject.put(timestamp, "2015-05-12T00:59:00Z");
		sourceObject.put(test30, 30);
		sourceObject.put(test400, 400);

		Map<String, Object> sourceObject2 = new HashMap<String, Object>();
		sourceObject2.put(timestamp, "2015-05-12T00:59:02Z");
		sourceObject2.put(test30, 30);
		sourceObject2.put(test400, 400);

		Map<String, Object> sourceObject3 = new HashMap<String, Object>();
		sourceObject3.put(timestamp, "2015-05-12T01:00:00Z");
		sourceObject3.put(test30, 30);
		sourceObject3.put(test400, 400);

		if (!jmElasticsearchClient.isExists(index))
			assertTrue(jmElasticsearchClient.create(index));
		jmElasticsearchClient.sendData(sourceObject, index, type);
		jmElasticsearchClient.sendData(sourceObject2, index, type);
		jmElasticsearchClient.sendData(sourceObject3, index, type);
		// 인덱싱할 시간 필요
		JMThread.sleep(5000);

		SearchResponse searchResponse1 =
				jmElasticsearchClient.searchAll(indices, types);
		System.out.println(searchResponse1);

		AbstractAggregationBuilder[] sumAggregationBuilders =
				new AbstractAggregationBuilder[sumFields.length];
		for (int i = 0; i < sumFields.length; i++)
			sumAggregationBuilders[i] =
					AggregationBuilders.stats(sumFields[i]).field(sumFields[i]);
		searchResponse1 = jmElasticsearchClient.searchAll(indices, types,
				sumAggregationBuilders);
		assertTrue(searchResponse1.toString().contains("\"count\" : 3"));
		assertTrue(searchResponse1.toString().contains("\"sum\" : 90.0"));
		RangeFilterBuilder dataRangeFilter =
				FilterBuilders.rangeFilter(timestamp)
						.gte("2015-05-12T00:59:00Z").lt("2015-05-12T01:00:00Z");
		System.out.println(jmElasticsearchClient.searchAll(indices));
		searchResponse1 = jmElasticsearchClient.searchAll(indices, types,
				dataRangeFilter);
		assertTrue(searchResponse1.toString().contains("\"total\" : 2"));

		searchResponse1 = jmElasticsearchClient.searchAll(indices, types,
				dataRangeFilter, sumAggregationBuilders);
		assertTrue(searchResponse1.toString().contains("\"count\" : 2"));
		assertTrue(searchResponse1.toString().contains("\"sum\" : 60.0"));

	}

	@Test
	public final void testGetIndexList() {
		Set<String> allIndexList = jmElasticsearchClient.getAllIndices();
		System.out.println(allIndexList);

		String containedString = "test";
		Set<String> indexList =
				jmElasticsearchClient.getIndexList(containedString);
		System.out.println(indexList);
	}

	@Test
	public final void testDeleteIndices() {
		Set<String> allIndexList = jmElasticsearchClient.getAllIndices();
		System.out.println(allIndexList);

		String containedString = "test-";

		Set<String> indexList =
				jmElasticsearchClient.getIndexList(containedString);
		System.out.println(indexList);

		jmElasticsearchClient
				.deleteIndices(indexList.toArray(new String[indexList.size()]));
		JMThread.sleep(1000);
		allIndexList = jmElasticsearchClient.getAllIndices();
		System.out.println(allIndexList);
		assertEquals(0, indexList.size());
		assertEquals(0, allIndexList.size());

		indexList = jmElasticsearchClient.getIndexList(containedString);
		System.out.println(indexList);

	}

}
