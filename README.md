JMLab Utility Libraries For Elasticsearch 7
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
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/kr.jmlab/jmlab-utils-elasticsearch/badge.svg)](http://search.maven.org/#artifactdetails%7Ckr.jmlab%7Cjmlab-utils-elasticsearch%7C7.10.2%7Cjar)

## Prerequisites:
* Java 11 or later

## Installation

Checkout the source code:

    https://github.com/JM-Lab/utils-elasticsearch.git
    cd utils-elasticsearch
    git checkout -b 7.10.2 origin/7.10.2
    mvn install

## Usage
Set up pom.xml :

    (...)
    <dependency>
			<groupId>kr.jmlab</groupId>
			<artifactId>jmlab-utils-elasticsearch</artifactId>
			<version>7.10.2</version>
	</dependency>
    (...)

For example ([JMElasticsearchClientTest.java](https://github.com/JM-Lab/utils-elasticsearch/blob/master/src/test/java/kr/jm/utils/elasticsearch/JMElasticsearchClientTest.java)) :

```java
// Embedded Elasticsearch Node Start
this.jmEmbeddedElasticsearch = new JMEmbeddedElasticsearch();
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
