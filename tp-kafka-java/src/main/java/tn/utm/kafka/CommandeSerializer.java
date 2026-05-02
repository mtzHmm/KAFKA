package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Exercice 4.C (Bonus) — Sérialiseur Kafka custom pour Commande.
 * Enregistré directement dans la config du producer ; aucun mapping
 * JSON manuel dans le code métier.
 */
public class CommandeSerializer implements Serializer<Commande> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Override
    public byte[] serialize(String topic, Commande commande) {
        if (commande == null) return null;
        try {
            return MAPPER.writeValueAsBytes(commande);
        } catch (Exception e) {
            throw new RuntimeException("Erreur sérialisation Commande", e);
        }
    }
}
