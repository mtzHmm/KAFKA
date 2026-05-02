package tn.utm.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Partie 4.3 — Premier consommateur Kafka.
 * Lit en continu depuis le topic "ventes", groupe "groupe-java-1".
 *
 * Lancer : mvn exec:java -Dexec.mainClass="tn.utm.kafka.SimpleConsumer"
 */
public class SimpleConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,       "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                "groupe-java-1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // earliest = depuis le début si aucun offset committé
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,   "earliest");
        // Commit manuel — plus fiable qu'auto-commit
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,  false);

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("ventes"));

            System.out.println("  En attente de messages... (Ctrl+C pour arrêter)");

            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf(
                            "  partition=%d, offset=%d, key=%s, value=%s%n",
                            record.partition(), record.offset(),
                            record.key(), record.value());
                }

                // Commit synchrone uniquement si des messages ont été traités
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        }
    }
}
