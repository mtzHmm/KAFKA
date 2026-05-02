package tn.utm.kafka;

/**
 * Partie 4.2 — Objet métier représentant une vente.
 * Utilisé avec la sérialisation JSON Jackson.
 */
public class Vente {

    private String idClient;
    private double montant;
    private String ville;

    public Vente() {}

    public Vente(String idClient, double montant, String ville) {
        this.idClient = idClient;
        this.montant  = montant;
        this.ville    = ville;
    }

    public String getIdClient() { return idClient; }
    public void setIdClient(String idClient) { this.idClient = idClient; }

    public double getMontant() { return montant; }
    public void setMontant(double montant) { this.montant = montant; }

    public String getVille() { return ville; }
    public void setVille(String ville) { this.ville = ville; }

    @Override
    public String toString() {
        return "Vente{idClient='" + idClient + "', montant=" + montant + ", ville='" + ville + "'}";
    }
}
