package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Exercice 4.C (Bonus) — Désérialiseur Kafka custom pour Commande.
 */
public class CommandeDeserializer implements Deserializer<Commande> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Override
    public Commande deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try {
            return MAPPER.readValue(data, Commande.class);
        } catch (Exception e) {
            throw new RuntimeException("Erreur désérialisation Commande", e);
        }
    }
}
