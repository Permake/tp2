package fr.uge.bigdata;

import fr.uge.bigdata.data.Etudiant;
import fr.uge.bigdata.serializer.EtudiantDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerJsonKafka {
    static void main(String[] args) {
        // Set up the consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group-id");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EtudiantDeserializer.class.getName());

        // Create the consumer
        Consumer<String, Etudiant> consumer = new KafkaConsumer<>(props);

        // Continuously poll for new messages
        try (consumer) {
            consumer.subscribe(Collections.singletonList("Messages"));
            while (true) {
                ConsumerRecords<String, Etudiant> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, Etudiant> record : records) {
                    System.out.printf("Partition: %d | Valeur: %s %s%n",
                            record.partition(),
                            record.value().firstName(),
                            record.value().lastName());
                }
            }
        }
        // Close the consumer
    }
}
