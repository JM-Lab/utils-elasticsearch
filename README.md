JMLab Utility Libraries For Elasticsearch Client
================================================

Extends The Elasticsearch 1.7 Client

## version
	0.1.7

## Prerequisites:
* Java 8 or later

## Installation

Checkout the source code:

    https://github.com/JM-Lab/utils-elasticsearch.git
    cd utils-elasticsearch
    git checkout -b 0.1.7 origin/0.1.7
    mvn install

## Usage
Set up pom.xml :

    (...)
    <dependency>
			<groupId>jmlab</groupId>
			<artifactId>jmlab-utils-elasticsearch</artifactId>
			<version>0.1.7</version>
	</dependency>
    (...)

For example ([JMElasticsearchClientTest.java](https://github.com/JM-Lab/utils-elasticsearch/blob/master/src/test/java/kr/jm/utils/elasticsearch/JMElasticsearchClientTest.java)) :

```java
// Elasticsearch local data node start
this.elasticsearch = NodeBuilder.nodeBuilder().build().start();

// transportClient init
String ipPortAsCsv = "localhost:9300";
this.jmElasticsearchClient = new JMElasticsearchClient(ipPortAsCsv);
		
// nodeClient init
boolean isTransportClient = false;
ipPortAsCsv = "localhost:9300,127.0.0.1:9300";
boolean clientTransportSniff = false;
this.jmElasticsearchNodeClient = new JMElasticsearchClient(isTransportClient, ipPortAsCsv, clientTransportSniff);

// set to -1 to disable it
int bulkActions = 3;
// set to -1 to disable it, ex) 100KB, 1m, 1gb or 1g ...
String bulkSize = "1MB";
int flushIntervalSeconds = 5;
this.jmElasticsearchClient.setBulkProcessor(bulkActions, bulkSize, flushIntervalSeconds);
```

## Useful Functions :
* **Create Elasticsearch Client (Transport Or Node)**
* **SearchQuery**
* **SearchQueryBuilder**
* **CountQuery**
* **DeleteQuery**
* **BulkProcessor**
* **etc.**
