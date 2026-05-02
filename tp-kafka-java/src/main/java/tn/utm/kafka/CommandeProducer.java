package tn.utm.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Exercice 4.C — Producteur de commandes sérialisées en JSON via CommandeSerializer.
 *
 * Lancer : mvn exec:java -Dexec.mainClass="tn.utm.kafka.CommandeProducer"
 */
public class CommandeProducer {

    private static final String TOPIC = "commandes";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        // Bonus : sérialiseur custom — pas de mapping JSON dans le code métier
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CommandeSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,                   "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,     true);

        List<Commande> commandes = buildSampleCommandes();

        try (Producer<String, Commande> producer = new KafkaProducer<>(props)) {
            for (Commande commande : commandes) {
                ProducerRecord<String, Commande> record =
                        new ProducerRecord<>(TOPIC, commande.getId(), commande);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Échec envoi : " + exception.getMessage());
                    } else {
                        System.out.printf("  Envoyé — id=%s, partition=%d, offset=%d, total=%.2f DT%n",
                                commande.getId(), metadata.partition(),
                                metadata.offset(), commande.getTotal());
                    }
                });
            }
            producer.flush();
            System.out.println("Toutes les commandes ont été envoyées.");
        }
    }

    private static List<Commande> buildSampleCommandes() {
        return Arrays.asList(
            new Commande(UUID.randomUUID().toString(), LocalDateTime.now(),
                    Arrays.asList("pain", "lait"),        45.00),
            new Commande(UUID.randomUUID().toString(), LocalDateTime.now(),
                    Arrays.asList("café", "sucre", "eau"), 82.50),
            new Commande(UUID.randomUUID().toString(), LocalDateTime.now(),
                    Arrays.asList("fromage", "huile"),     130.75),
            new Commande(UUID.randomUUID().toString(), LocalDateTime.now(),
                    Arrays.asList("farine", "lait", "pain", "café"), 220.00)
        );
    }
}
