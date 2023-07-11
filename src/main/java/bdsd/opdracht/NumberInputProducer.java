package bdsd.opdracht;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;
import java.util.Random;

public class NumberInputProducer {

    public static void main(String[] args) {
        Properties producerProperties = new Properties();
        producerProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-number-producer");
        producerProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        Random random = new Random();
        String key = Integer.toString(random.nextInt(10000));
        String numberList = NumberInputProducer.generateRandomInputString();

        ProducerRecord<String, String> record = new ProducerRecord<>("streams-number-input", key, numberList);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Error sending average message: [key]" + key + exception.getMessage());
            } else {
                System.out.println("Message sent successfully to topic: [topic:key:numbers] " + metadata.topic() + ":" + key + ":" + numberList);
            }
        });

        producer.close();
    }

    public static String generateRandomInputString() {
        Random random = new Random();
        StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < 50; i++) {
            int num = random.nextInt(1000 - 1 + 1) + 1;
            stringBuilder.append(num);
            if (i < 50 - 1) {
                stringBuilder.append(",");
            }
        }

        return stringBuilder.toString();
    }

}


