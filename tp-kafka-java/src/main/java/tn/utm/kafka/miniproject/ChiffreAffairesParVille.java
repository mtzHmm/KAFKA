package tn.utm.kafka.miniproject;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Partie 6 — Étape 3 : Calcul du chiffre d'affaires par ville.
 *
 * Consomme le topic "pos-events", maintient en mémoire la somme
 * (VENTE - RETOUR) par ville et l'affiche toutes les 5 secondes.
 *
 * Groupe : ca-1
 *
 * Lancer : mvn exec:java -Dexec.mainClass="tn.utm.kafka.miniproject.ChiffreAffairesParVille"
 */
public class ChiffreAffairesParVille {

    private static final String TOPIC = "pos-events";
    private static final String GROUP = "ca-1";

    // CA cumulé par ville (accès depuis deux threads → synchronized)
    private static final Map<String, Double> caParVille = new HashMap<>();

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                  GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,    StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,         "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,        false);

        ObjectMapper mapper = new ObjectMapper();

        // Affichage périodique toutes les 5 secondes
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(
                ChiffreAffairesParVille::afficherCA, 5, 5, TimeUnit.SECONDS);

        System.out.println("ChiffreAffairesParVille démarré (groupe=" + GROUP + "). Ctrl+C pour arrêter.");

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                scheduler.shutdown();
                System.out.println("Consommateur CA arrêté.");
            }));

            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        PosEvent event = mapper.readValue(record.value(), PosEvent.class);
                        traiterEvent(event);
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

    private static synchronized void traiterEvent(PosEvent event) {
        if (event.getType() == PosEvent.Type.VENTE) {
            caParVille.merge(event.getVille(), event.getMontant(), Double::sum);
        } else if (event.getType() == PosEvent.Type.RETOUR) {
            caParVille.merge(event.getVille(), -event.getMontant(), Double::sum);
        }
        // OUVERTURE n'impacte pas le CA
    }

    private static synchronized void afficherCA() {
        System.out.println("\n--- Chiffre d'affaires par ville ---");
        if (caParVille.isEmpty()) {
            System.out.println("  (aucune donnée reçue)");
        } else {
            caParVille.entrySet().stream()
                    .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                    .forEach(e -> System.out.printf("  %-12s : %10.2f DT%n", e.getKey(), e.getValue()));
        }
        System.out.println("------------------------------------");
    }
}
