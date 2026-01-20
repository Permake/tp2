package fr.uge.bigdata;

import fr.uge.bigdata.data.Etudiant;

import fr.uge.bigdata.serializer.EtudiantSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerJsonKafka {
    static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Utilisation de notre sérialiseur personnalisé
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EtudiantSerializer.class.getName());

        KafkaProducer<String, Etudiant> producer = new KafkaProducer<>(props);

        Etudiant jean = new Etudiant("Jean", "Dupont", 21, "IT");
        
        producer.send(new ProducerRecord<>("etudiants", jean.lastName(), jean), (metadata, e) -> {
            if (e == null) {
                System.out.println("Envoyé à la partition : " + metadata.partition());
            }
        });

        producer.close();
    }
}