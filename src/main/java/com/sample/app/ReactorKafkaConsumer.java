package com.sample.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ReactorKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(ReactorKafkaConsumer.class.getName());

    ReceiverOptions<Integer, String> receiverOptions;
    private final SimpleDateFormat dateFormat;

    public ReactorKafkaConsumer(Properties properties) {
        receiverOptions = ReceiverOptions.create(properties);
        dateFormat = new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy");

    }

    public Disposable consumeMessages(String topic, CountDownLatch latch) {
        ReceiverOptions<Integer, String> options = receiverOptions.subscription(Collections.singleton(topic))
                .addAssignListener(partitions -> log.debug("onPartitionsAssigned {}", partitions))
                .addRevokeListener(partitions -> log.debug("onPartitionsRevoked {}", partitions));

        Flux<ReceiverRecord<Integer, String>> kafkaFlux = KafkaReceiver.create(options).receive();
        return kafkaFlux.subscribe(record -> {
                    ReceiverOffset offset = record.receiverOffset();
                    System.out.printf("Received message: topic-partition=%s offset=%d timestamp=%s key=%d value=%s\n",
                            offset.topicPartition(),
                            offset.offset(),
                            dateFormat.format(new Date(record.timestamp())),
                            record.key(),
                            record.value());
                    try {
                        Thread.sleep(1 * 1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    offset.acknowledge();
                    latch.countDown();
                }
        );
    }

//    public static void main(String[] args) throws Exception {
//        int count = 20;
//        CountDownLatch latch = new CountDownLatch(count);
//        ReactorKafkaConsumer consumer = new SampleConsumer(BOOTSTRAP_SERVERS);
//        Disposable disposable = consumer.consumeMessages(TOPIC, latch);
//        latch.await(10, TimeUnit.SECONDS);
//        disposable.dispose();
//    }
}
