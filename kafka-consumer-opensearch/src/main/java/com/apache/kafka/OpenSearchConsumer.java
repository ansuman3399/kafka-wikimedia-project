package com.apache.kafka;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {
    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getName());
        String indexName = "wikimedia";
        //create opensearch client
        RestHighLevelClient opensearchClient = createOpenSearchClient();
        //create kafka client
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();
        //we need to create the index on opensearch if it doesnt exist
        boolean isIndexExists = opensearchClient.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
        if (!isIndexExists) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
            opensearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            log.info("Wikimedia index is created!");
        } else {
            log.info("Index already exists");
        }
        //Subscribing to the topic
        kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchange"));

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));
            log.info("Received:" + records.count());
            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record : records) {
                //use an ID to attach to a record to avoid duplicate processing i.e,making the consumer idempotent
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                //or you can use the ID if its present in the message - might not be present
                String id = extractId(record.value());
                try {
                    //Send the record into opensearch
                    IndexRequest indexRequest = new IndexRequest(indexName)
                            .source(record.value(), XContentType.JSON)
                            .id(id);

//                    IndexResponse response = opensearchClient.index(indexRequest, RequestOptions.DEFAULT);
                    bulkRequest.add(indexRequest);
//                    log.info("Inserted 1 document into opensearch with ID:" + response.getId());
                } catch (Exception e) {

                }
            }
            if(bulkRequest.numberOfActions()>0){
                BulkResponse bulkResponse = opensearchClient.bulk(bulkRequest,RequestOptions.DEFAULT);
                log.info("Inserted:"+bulkResponse.getItems().length + " record(s)");

                try{
                    Thread.sleep(1000);
                }catch (InterruptedException e){

                }

                kafkaConsumer.commitSync();
                log.info("Commited the offsets");
            }
        }
        //main code logic


        //
    }

    public static String extractId(String value) {
        return JsonParser.parseString(value)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static KafkaConsumer<String, String> createKafkaConsumer() {
        String bootStrapserver = "127.0.0.1:9092";
        String groupId = "consumer-opensearch-demo";

        //create consumer configurations
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapserver);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        return consumer;
    }

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";

        RestHighLevelClient restHighLevelClient;
        //Build URI from the connection string
        URI connUri = URI.create(connString);
        //Extract login info if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
        } else {
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                            )
            );

        }
        return restHighLevelClient;
    }
}
