package bdsd.opdracht;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Implementation is based on the Kafka Streams example apps provided by Apache Kafka
 * https://kafka.apache.org/35/documentation/streams/tutorial
 */
public class NumberProcessor {

    public static void main(String[] args) {
        Properties producerProperties = new Properties();
        producerProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-number-producer");
        producerProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        Properties consumerProps = new Properties();
        consumerProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-number-consumer");
        consumerProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        consumerProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream("streams-number-input");

        inputStream.foreach((key, value) -> {
            System.out.println("Message received: [key:value] " + key + ":" + value);

            ArrayList<Integer> numberList = NumberProcessor.parseStringToIntegerList(value);
            double average = NumberProcessor.calculateAverage(numberList);
            double standardDeviation = NumberProcessor.calculateStandardDeviation(average, numberList);

            ProducerRecord<String, String> averageRecord = new ProducerRecord<>("streams-number-average", key, Double.toString(average));
            ProducerRecord<String, String> standardDeviationRecord = new ProducerRecord<>("streams-number-standard-deviation", key, Double.toString(standardDeviation));

            producer.send(averageRecord, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error sending average message: [key]" + key + exception.getMessage());
                } else {
                    System.out.println("Message sent successfully to topic: [topic:average]" + metadata.topic() + ":" + average);
                }
            });

            producer.send(standardDeviationRecord, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error sending standard deviation message: [key]" + key + exception.getMessage());
                } else {
                    System.out.println("Message sent successfully to topic: [topic:standarddeviation] " + metadata.topic() + ":" + standardDeviation);
                }
            });
        });

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, consumerProps);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                producer.close();
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

    private static ArrayList<Integer> parseStringToIntegerList(String numberString) {
        ArrayList<Integer> numberList = new ArrayList<>();
        String[] numberStringArray = numberString.split(",");
        for (String x : numberStringArray) {
            numberList.add(Integer.parseInt(x));
        }

        return numberList;
    }

    private static double calculateAverage(ArrayList<Integer> numbers) {
        int sum = 0;
        for (int number : numbers) {
            sum += number;
        }
        return (double) sum / numbers.size();
    }

    private static double calculateStandardDeviation(double average, ArrayList<Integer> numbers) {
        double standardDeviation = 0.0;
        for (Integer number : numbers) {
            standardDeviation += Math.pow(number - average, 2);
        }

        return Math.sqrt(standardDeviation / numbers.size());
    }
}
