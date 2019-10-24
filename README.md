JMLab Utility Libraries For Elasticsearch 6
===========================================

## Useful Functions :
* **Embedded Elasticsearch Node - JMEmbeddedElasticsearch**
* **Elasticsearch Client (Transport) - JMElasticsearchClient**
* ***SearchQuery***
* ***SearchQueryBuilder***
* ***CountQuery***
* ***DeleteQuery***
* ***BulkProcessor***

## version
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/kr.jmlab/jmlab-utils-elasticsearch/badge.svg)](http://search.maven.org/#artifactdetails%7Ckr.jmlab%7Cjmlab-utils-elasticsearch%7C6.8.3%7Cjar)

## Prerequisites:
* Java 8 or later

## Installation

Checkout the source code:

    https://github.com/JM-Lab/utils-elasticsearch.git
    cd utils-elasticsearch
    git checkout -b 6.8.3 origin/6.8.3
    mvn install

## Usage
Set up pom.xml :

    (...)
    <dependency>
			<groupId>kr.jmlab</groupId>
			<artifactId>jmlab-utils-elasticsearch</artifactId>
			<version>6.8.3</version>
	</dependency>
    (...)

For example ([JMElasticsearchClientTest.java](https://github.com/JM-Lab/utils-elasticsearch/blob/master/src/test/java/kr/jm/utils/elasticsearch/JMElasticsearchClientTest.java)) :

```java
// Embedded Elasticsearch Node Start
this.jmEmbeddedElasticsearch = new JMEmbeddedElastricsearch();
this.jmEmbeddedElasticsearch.start();
		
// JMElasticsearchClient Init
this.jmElasticsearchClient = new JMElasticsearchClient(
this.jmEmbeddedElasticsearch.getTransportIpPortPair());
		
// Bulk Processor Setting
int bulkActions = 3;
long bulkSizeKB = 50;
int flushIntervalSeconds = 5;
this.jmElasticsearchClient.setBulkProcessor(bulkActions, bulkSizeKB, flushIntervalSeconds);
```
