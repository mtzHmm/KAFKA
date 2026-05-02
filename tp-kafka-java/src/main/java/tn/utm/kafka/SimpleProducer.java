package tn.utm.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
// props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

/**
 * Partie 4.2 — Premier producteur Kafka.
 * Envoie 10 messages avec clé vers le topic "ventes".
 *
 * Lancer : mvn exec:java -Dexec.mainClass="tn.utm.kafka.SimpleProducer"
 */
public class SimpleProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Garanties de durabilité
        props.put(ProducerConfig.ACKS_CONFIG,                  "all");
        props.put(ProducerConfig.RETRIES_CONFIG,               3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,    true);

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= 10; i++) {
                String key   = "client-" + (i % 3);
                String value = "Achat numéro " + i + " (" + (50 + i * 17) + " DT)";

                ProducerRecord<String, String> record =
                        new ProducerRecord<>("ventes", key, value);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("ERREUR: " + exception.getClass().getSimpleName() + " - " + exception.getMessage());
                    } else {
                        System.out.println("  Envoye [OK] partition=" + metadata.partition() +
                                ", offset=" + metadata.offset() +
                                ", key=" + record.key());
                    }
                });

                try {
    Thread.sleep(500);
} catch (InterruptedException e) {
    e.printStackTrace();
}
            }
            producer.flush();
            System.out.println("Tous les messages ont été envoyés.");
        }
    }
}
