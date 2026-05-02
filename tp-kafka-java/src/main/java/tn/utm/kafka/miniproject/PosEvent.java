package tn.utm.kafka.miniproject;

import java.util.List;

/**
 * Modèle d'un événement émis par une caisse POS.
 *
 * Format JSON :
 * {
 *   "type": "VENTE",
 *   "idCaisse": "CAISSE-TUNIS-03",
 *   "ville": "Tunis",
 *   "timestamp": "2026-04-28T14:35:12.402Z",
 *   "montant": 175.50,
 *   "produits": ["pain", "lait", "fromage"]
 * }
 */
public class PosEvent {

    public enum Type { VENTE, RETOUR, OUVERTURE }

    private Type type;
    private String idCaisse;
    private String ville;
    private String timestamp;
    private double montant;
    private List<String> produits;

    public PosEvent() {}

    public PosEvent(Type type, String idCaisse, String ville,
                    String timestamp, double montant, List<String> produits) {
        this.type      = type;
        this.idCaisse  = idCaisse;
        this.ville     = ville;
        this.timestamp = timestamp;
        this.montant   = montant;
        this.produits  = produits;
    }

    public Type getType()             { return type; }
    public void setType(Type type)    { this.type = type; }

    public String getIdCaisse()              { return idCaisse; }
    public void   setIdCaisse(String v)      { this.idCaisse = v; }

    public String getVille()                 { return ville; }
    public void   setVille(String v)         { this.ville = v; }

    public String getTimestamp()             { return timestamp; }
    public void   setTimestamp(String v)     { this.timestamp = v; }

    public double getMontant()               { return montant; }
    public void   setMontant(double v)       { this.montant = v; }

    public List<String> getProduits()               { return produits; }
    public void         setProduits(List<String> v) { this.produits = v; }

    @Override
    public String toString() {
        return "PosEvent{type=" + type + ", caisse='" + idCaisse +
               "', ville='" + ville + "', montant=" + montant + "}";
    }
}
