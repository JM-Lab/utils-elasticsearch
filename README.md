JMLab Utility Libraries For Elasticsearch 5
===========================================

## Useful Functions :
* **Embeded Elasticsearch Node - JMEmbededElastricsearch**
* **Elasticsearch Client (Transport) - JMElasticsearchClient**
* ***SearchQuery***
* ***SearchQueryBuilder***
* ***CountQuery***
* ***DeleteQuery***
* ***BulkProcessor***

## version
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.jm-lab/jmlab-utils-elasticsearch/badge.svg)](http://search.maven.org/#artifactdetails%7Ccom.github.jm-lab%7Cjmlab-utils-elasticsearch%7C0.5.1%7Cjar)

## Prerequisites:
* Java 8 or later

## Installation

Checkout the source code:

    https://github.com/JM-Lab/utils-elasticsearch.git
    cd utils-elasticsearch
    git checkout -b 0.5.1 origin/0.5.1
    mvn install

## Usage
Set up pom.xml :

    (...)
    <dependency>
			<groupId>com.github.jm-lab</groupId>
			<artifactId>jmlab-utils-elasticsearch</artifactId>
			<version>0.5.1</version>
	</dependency>
    (...)

For example ([JMElasticsearchClientTest.java](https://github.com/JM-Lab/utils-elasticsearch/blob/master/src/test/java/kr/jm/utils/elasticsearch/JMElasticsearchClientTest.java)) :

```java
// Embeded Elasticsearch Node Start
this.jmEmbededElasticsearch = new JMEmbededElastricsearch();
this.jmEmbededElasticsearch.start();
		
// JMElasticsearchClient Init
this.jmElasticsearchClient = new JMElasticsearchClient(
this.jmEmbededElasticsearch.getTransportIpPortPair());
		
// Bulk Processor Setting
int bulkActions = 3;
long bulkSizeKB = 5 * 1024;
int flushIntervalSeconds = 5;
this.jmElasticsearchClient.setBulkProcessor(bulkActions, bulkSizeKB, flushIntervalSeconds);
```
