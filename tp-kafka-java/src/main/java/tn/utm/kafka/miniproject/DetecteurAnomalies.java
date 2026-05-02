package tn.utm.kafka.miniproject;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Partie 6 — Étape 4 : Détecteur d'anomalies de retours.
 *
 * Lit le topic "pos-events" (groupe "alerte-1").
 * Pour chaque RETOUR dont le montant > 200 DT, publie une alerte
 * dans le topic "alertes-retours".
 *
 * Lancer : mvn exec:java -Dexec.mainClass="tn.utm.kafka.miniproject.DetecteurAnomalies"
 */
public class DetecteurAnomalies {

    private static final String TOPIC_SOURCE = "pos-events";
    private static final String TOPIC_ALERTES = "alertes-retours";
    private static final String GROUP         = "alerte-1";
    private static final double SEUIL_RETOUR  = 200.0;

    public static void main(String[] args) throws Exception {

        // --- Consumer ---
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,                  GROUP);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,    StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,         "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,        false);

        // --- Producer (pour écrire les alertes) ---
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG,                   "all");
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,     true);

        ObjectMapper mapper = new ObjectMapper();

        System.out.println("DetecteurAnomalies démarré (groupe=" + GROUP + "). Ctrl+C pour arrêter.");

        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             Producer<String, String> producer = new KafkaProducer<>(producerProps)) {

            consumer.subscribe(Collections.singletonList(TOPIC_SOURCE));

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                producer.flush();
                System.out.println("DetecteurAnomalies arrêté proprement.");
            }));

            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        PosEvent event = mapper.readValue(record.value(), PosEvent.class);

                        if (event.getType() == PosEvent.Type.RETOUR
                                && event.getMontant() > SEUIL_RETOUR) {

                            String alerte = String.format(
                                    "ALERTE RETOUR ANORMAL — caisse=%s, ville=%s, montant=%.2f DT, ts=%s",
                                    event.getIdCaisse(), event.getVille(),
                                    event.getMontant(), event.getTimestamp());

                            System.out.println("[ANOMALIE] " + alerte);

                            producer.send(
                                    new ProducerRecord<>(TOPIC_ALERTES, event.getVille(),
                                            mapper.writeValueAsString(event)),
                                    (meta, ex) -> {
                                        if (ex != null) {
                                            System.err.println("Erreur envoi alerte : " + ex.getMessage());
                                        }
                                    }
                            );
                        }

                    } catch (Exception e) {
                        System.err.println("Erreur parsing : " + e.getMessage());
                    }
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        }
    }
}
