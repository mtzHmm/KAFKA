# TP Apache Kafka — Déploiement mono-machine

Projet Maven Java couvrant les Parties 4 et 6 du TP.

---

## Prérequis

| Outil | Version minimale |
|-------|-----------------|
| Java JDK | 17 |
| Maven | 3.8+ |
| Apache Kafka | 3.9.x (mode KRaft) |

---

## Démarrage de Kafka (KRaft, sans ZooKeeper)

```powershell
# Windows — une seule fois (formatage du stockage)
$env:KAFKA_HOME = "C:\kafka"
$env:Path = "$env:KAFKA_HOME\bin\windows;$env:Path"

$KAFKA_CLUSTER_ID = kafka-storage.bat random-uuid
kafka-storage.bat format --config C:\kafka-data\server.properties --cluster-id $KAFKA_CLUSTER_ID

# Démarrage du broker
kafka-server-start.bat -daemon C:\kafka-data\server.properties
```

---

## Création des topics nécessaires

```bash
# Topic pour les parties 4.x
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ventes --partitions 3 --replication-factor 1

# Topics du mini-projet (Partie 6)
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic pos-events    --partitions 4 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic alertes-retours --partitions 2 --replication-factor 1
```

---

## Compilation

```bash
mvn clean package
```

---

## Exécution des classes

### Partie 4 — Producer / Consumer basiques

```bash
# Terminal 1 — Consumer
mvn exec:java -Dexec.mainClass="tn.utm.kafka.SimpleConsumer"

# Terminal 2 — Producer
mvn exec:java -Dexec.mainClass="tn.utm.kafka.SimpleProducer"
```

### Partie 6 — Mini-projet Pipeline POS

```bash
# Terminal 1 — Simulateur caisse 1
mvn exec:java -Dexec.mainClass="tn.utm.kafka.miniproject.SimulateurCaisse" -Dexec.args="CAISSE-TUNIS-01"

# Terminal 2 — Simulateur caisse 2 (optionnel)
mvn exec:java -Dexec.mainClass="tn.utm.kafka.miniproject.SimulateurCaisse" -Dexec.args="CAISSE-SOUSSE-01"

# Terminal 3 — Chiffre d'affaires par ville
mvn exec:java -Dexec.mainClass="tn.utm.kafka.miniproject.ChiffreAffairesParVille"

# Terminal 4 — Détecteur d'anomalies
mvn exec:java -Dexec.mainClass="tn.utm.kafka.miniproject.DetecteurAnomalies"
```

---

## Surveiller le LAG

```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group ca-1
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group alerte-1
```

---

## Structure du projet

```
tp-kafka-java/
├── pom.xml
├── README.md
└── src/main/java/tn/utm/kafka/
    ├── SimpleProducer.java          # Partie 4.2
    ├── SimpleConsumer.java          # Partie 4.3
    ├── Vente.java                   # Partie 4.2 — objet métier
    ├── Commande.java                # Exercice 4.C — objet métier
    └── miniproject/
        ├── PosEvent.java            # Modèle d'événement POS
        ├── SimulateurCaisse.java    # Partie 6 — Étape 2
        ├── ChiffreAffairesParVille.java  # Partie 6 — Étape 3
        └── DetecteurAnomalies.java  # Partie 6 — Étape 4
```
