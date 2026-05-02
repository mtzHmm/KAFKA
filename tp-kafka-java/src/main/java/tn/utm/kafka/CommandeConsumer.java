package tn.utm.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Exercice 4.C — Consommateur de commandes, désérialisées via CommandeDeserializer.
 *
 * Lancer : mvn exec:java -Dexec.mainClass="tn.utm.kafka.CommandeConsumer"
 */
public class CommandeConsumer {

    private static final String TOPIC = "commandes";
    private static final String GROUP = "groupe-commandes-1";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                  GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,    StringDeserializer.class.getName());
        // Bonus : désérialiseur custom
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,  CommandeDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,         "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,        false);

        try (Consumer<String, Commande> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            System.out.println("En attente de commandes... (Ctrl+C pour arrêter)");

            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, Commande> records =
                        consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, Commande> record : records) {
                    Commande cmd = record.value();
                    System.out.printf(
                            "  [partition=%d offset=%d] Commande reçue :%n" +
                            "    id       : %s%n" +
                            "    date     : %s%n" +
                            "    articles : %s%n" +
                            "    total    : %.2f DT%n",
                            record.partition(), record.offset(),
                            cmd.getId(), cmd.getDate(),
                            cmd.getArticles(), cmd.getTotal());
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        }
    }
}
