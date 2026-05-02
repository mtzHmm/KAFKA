package tn.utm.kafka.miniproject;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Partie 6 — Étape 2 : Simulateur de caisses POS.
 *
 * Émet en continu des événements VENTE / RETOUR / OUVERTURE vers le topic "pos-events".
 * La clé de chaque message est la ville (garantit l'ordre intra-ville).
 *
 * Lancer : mvn exec:java -Dexec.mainClass="tn.utm.kafka.miniproject.SimulateurCaisse"
 *          mvn exec:java -Dexec.mainClass="tn.utm.kafka.miniproject.SimulateurCaisse" -Dexec.args="CAISSE-SOUSSE-01"
 */
public class SimulateurCaisse {

    private static final String TOPIC = "pos-events";

    private static final String[] VILLES  = {"Tunis", "Sousse", "Sfax", "Bizerte", "Gabès"};
    private static final List<String> PRODUITS =
            Arrays.asList("pain", "lait", "fromage", "eau", "café", "sucre", "farine", "huile");

    public static void main(String[] args) throws Exception {

        String idCaisse = (args.length > 0) ? args[0] : "CAISSE-DEFAULT-01";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,                   "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,     true);

        ObjectMapper mapper = new ObjectMapper();
        Random       rng    = ThreadLocalRandom.current();

        System.out.println("Simulateur " + idCaisse + " démarré. Ctrl+C pour arrêter.");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                producer.flush();
                System.out.println("Simulateur " + idCaisse + " arrêté proprement.");
            }));

            while (!Thread.currentThread().isInterrupted()) {

                String ville = VILLES[rng.nextInt(VILLES.length)];
                PosEvent.Type type = pickType(rng);

                double montant = 0.0;
                List<String> produits = List.of();

                if (type == PosEvent.Type.VENTE || type == PosEvent.Type.RETOUR) {
                    montant  = 5.0 + rng.nextDouble() * 495.0;
                    montant  = Math.round(montant * 100.0) / 100.0;
                    produits = List.of(PRODUITS.get(rng.nextInt(PRODUITS.size())));
                }

                PosEvent event = new PosEvent(
                        type,
                        idCaisse,
                        ville,
                        Instant.now().toString(),
                        montant,
                        produits
                );

                String json = mapper.writeValueAsString(event);

                producer.send(
                        new ProducerRecord<>(TOPIC, ville, json),
                        (metadata, ex) -> {
                            if (ex != null) {
                                System.err.println("Erreur envoi : " + ex.getMessage());
                            } else {
                                System.out.printf("[%s] Envoyé → partition=%d offset=%d  %s%n",
                                        idCaisse, metadata.partition(), metadata.offset(), event);
                            }
                        }
                );

                // Délai aléatoire entre 100 ms et 500 ms
                Thread.sleep(100 + rng.nextInt(401));
            }
        }
    }

    /**
     * Distribution : VENTE 70 %, OUVERTURE 20 %, RETOUR 10 %.
     */
    private static PosEvent.Type pickType(Random rng) {
        int n = rng.nextInt(100);
        if (n < 70) return PosEvent.Type.VENTE;
        if (n < 90) return PosEvent.Type.OUVERTURE;
        return PosEvent.Type.RETOUR;
    }
}
