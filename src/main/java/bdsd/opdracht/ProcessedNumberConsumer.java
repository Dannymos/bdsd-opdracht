package bdsd.opdracht;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Implementation is based on the Kafka Streams example apps provided by Apache Kafka
 * https://kafka.apache.org/35/documentation/streams/tutorial
 */
public class ProcessedNumberConsumer {

    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-processed-number-consumer");
        consumerProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        consumerProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> averageStream = builder.stream("streams-number-average");
        KStream<String, String> standardDeviationStream = builder.stream("streams-number-standard-deviation");

        averageStream.foreach((key, value) -> {
            System.out.println("Message [average]  received: [key:value] " + key + ":" + value);
        });

        standardDeviationStream.foreach((key, value) -> {
            System.out.println("Message [Standard Deviation] received: [key:value] " + key + ":" + value);
        });

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, consumerProps);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
