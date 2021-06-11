package com.sample.app;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.Disposable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.sample.app.TestUtils.*;

public class SimpleTest {

    private static final Logger logger = LoggerFactory.getLogger(SimpleTest.class);

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"));

    public static final String TOPIC = "sample";
    public static final int NUM_PARTITIONS = 1;
    public static final int REPLICATION_FACTOR = 1;

    Properties consumerProperties;

    Properties producerProperties;

    AdminClient admin;


    @BeforeEach
    public void beforeEach() throws ExecutionException, InterruptedException {
        kafka.start();

        consumerProperties = toProperties(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, SimpleTest.class.getName() + Math.random(),
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000", //10s
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        ));

        producerProperties = toProperties(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        ));

        createTopic(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()),
                TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR, Collections.emptyMap()
        );
    }

    @Test
    public void puzzle() throws Exception {
        Map<Integer, String> messages = new HashMap<>();
        for (var i = 0; i < 1000; i++) {
            messages.put(i, "v" + i);
        }

        producerSendSync(producerProperties, TOPIC, messages);

        int count = 1000;
        CountDownLatch latch = new CountDownLatch(count);
        var consumer = new ReactorKafkaConsumer(consumerProperties);
        Disposable disposable = consumer.consumeMessages(TOPIC, latch);
        latch.await(1, TimeUnit.MINUTES);
        disposable.dispose();


//        var fromBeginningProperties = consumerProperties;
//        fromBeginningProperties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
//        fromBeginningProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//        var messagesFromBeginning = new HashMap<>();
//        consumerReadUntilMinKeyValueRecordsReceived(fromBeginningProperties, TOPIC, messages.size(), Duration.ofSeconds(10))
//                .forEach(record -> {
//                    messagesFromBeginning.put(record.key(), record.value());
//                });
//
//        assertThat(messagesFromBeginning.size()).isEqualTo(messages.size());
//        assertThat(messagesFromBeginning).containsAllEntriesOf(messages);
//
//
//        var fromOffset11Properties = consumerProperties;
//        fromBeginningProperties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
//
//        var messagesRecordsFromOffset11 = new HashMap<>();
//        consumerFromOffsetReadUntilMinKeyValueRecordsReceived(
//                fromOffset11Properties, TOPIC, 0, 11, 10, Duration.ofSeconds(10))
//                .forEach(record -> {
//                    messagesRecordsFromOffset11.put(record.key(), record.value());
//                });
//
//            assertThat(messagesRecordsFromOffset11).contains(
//                entry("k1", "v10"),
//                entry("k2", "v16"),
//                entry("k3", "v17"),
//                entry("k4", "v15")
//        );
    }
}
