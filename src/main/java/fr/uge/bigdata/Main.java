package fr.uge.bigdata;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(4);

        System.out.println(">>> Démarrage du système Kafka Multi-Threads...");
        executor.execute(() -> {
            System.out.println("[Thread] Lancement du Consommateur String...");
            BasicConsumerKafka.main(new String[]{});
        });

        executor.execute(() -> {
            System.out.println("[Thread] Lancement du Consommateur JSON...");
            ConsumerJsonKafka.main(new String[]{});
        });


        executor.execute(() -> {
            System.out.println("[Thread] Envoi de messages String...");
            BasicProducerKafka.main(new String[]{});
        });

        executor.execute(() -> {
            System.out.println("[Thread] Envoi d'étudiants JSON...");
            ProducerJsonKafka.main(new String[]{});
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Fermeture de l'application...");
            executor.shutdownNow();
        }));
    }
}