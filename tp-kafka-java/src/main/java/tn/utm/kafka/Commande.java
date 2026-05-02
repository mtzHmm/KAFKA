package tn.utm.kafka;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Partie 4.C — Objet métier représentant une commande.
 * Peut être sérialisé/désérialisé en JSON via Jackson.
 */
public class Commande {

    private String id;
    private LocalDateTime date;
    private List<String> articles;
    private double total;

    public Commande() {}

    public Commande(String id, LocalDateTime date, List<String> articles, double total) {
        this.id       = id;
        this.date     = date;
        this.articles = articles;
        this.total    = total;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public LocalDateTime getDate() { return date; }
    public void setDate(LocalDateTime date) { this.date = date; }

    public List<String> getArticles() { return articles; }
    public void setArticles(List<String> articles) { this.articles = articles; }

    public double getTotal() { return total; }
    public void setTotal(double total) { this.total = total; }

    @Override
    public String toString() {
        return "Commande{id='" + id + "', date=" + date +
               ", articles=" + articles + ", total=" + total + "}";
    }
}
