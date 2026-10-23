# EcommerceAnalytics

Projet d’analyse e-commerce distribué avec Scala, SBT et Apache Spark.

Le projet charge des données transactionnelles, valide leur qualité, enrichit les
référentiels clients / produits / marchands, applique les transformations métier,
calcule des KPI et produit des résultats structurés au format CSV et Parquet.

## 1. Prérequis

- Java 17
- Scala 2.12.18
- SBT 1.10.7
- Spark 3.5.5
- Git

Les dépendances Spark sont en portée `Provided`, ce qui permet de compiler le code
sans embarquer le runtime Spark dans le JAR final. Le runtime est fourni au moment
de l’exécution par `spark-submit` ou par le cluster.

## 2. Installation

### Linux / macOS

```sh
java -version
scala -version
sbt --version
```

### Windows

Installer Java 17, Scala et SBT, puis vérifier les variables d’environnement.
Pour l’exécution locale de Spark sur Windows, il est souvent nécessaire de configurer
`HADOOP_HOME` et d’ajouter `winutils.exe` dans le chemin, car Spark utilise des
bibliothèques Hadoop natives pour l’accès aux fichiers locaux.

Exemple de variables à configurer :

```powershell
$env:HADOOP_HOME = "C:\hadoop\winutils"
$env:PATH += ";$env:HADOOP_HOME\bin"
```

## 3. Structure du projet

```text
data_groupe3/
├── build.sbt
├── project/
├── src/
│   ├── main/
│   │   ├── resources/application.conf
│   │   └── scala/com/ecommerce/
│   └── test/scala/com/ecommerce/
├── data/
│   ├── transactions.csv
│   ├── users.json
│   ├── products.parquet
│   └── merchants.csv
├── output/
├── README.md
├── CONTRIBUTIONS.md
└── EQUIPE.md
```

## 4. Compilation et tests

À la racine du projet :

```sh
sbt clean compile
sbt test
```

Pour produire un JAR exécutable :

```sh
sbt assembly
```

Le JAR créé est :

```text
target/scala-2.12/ecommerce-analytics-assembly.jar
```

## 5. Exécution locale

Lancement via SBT :

```sh
sbt "run"
```

Le pipeline accepte une étape en argument :

- `ingestion`
- `transformation`
- `analytics`
- `all` (valeur par défaut)

Exemples :

```sh
sbt "run ingestion"
sbt "run transformation"
sbt "run analytics"
sbt "run all"
```

Le mode Spark local par défaut est `local[*]`; il peut être modifié dans
`src/main/resources/application.conf`.

## 6. Déploiement Spark

Après `sbt assembly`, il est possible d’exécuter le JAR avec `spark-submit` :

```sh
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master local[*] \
  target/scala-2.12/ecommerce-analytics-assembly.jar
```

Pour un cluster distribué, remplacer `local[*]` par la cible du cluster.

## 7. Données et qualité des données

Les fichiers de données attendus sont :

- `data/transactions.csv`
- `data/users.json`
- `data/products.parquet`
- `data/merchants.csv`

Le projet applique des règles de validation sur les colonnes métier, garde les
lignes rejetées avec une raison de rejet (`rejection_reason`) et génère un rapport
qualité exploitable.

Le rapport de qualité est écrit dans :

```text
output/data_quality_report/
```

## 8. Configuration

Les paramètres de configuration sont centralisés dans :

```text
src/main/resources/application.conf
```

On y retrouve :

- chemins des fichiers d’entrée / sortie,
- seuils de validation,
- master Spark,
- nombres de partitions,
- activation du cache et du broadcast.

Des valeurs de repli sont aussi définies dans `AppConfig` pour sécuriser le
chargement si une clé manque.

## 9. Sorties du pipeline

Les résultats sont écrits dans :

- `output/csv/`
- `output/parquet/`

Fichiers principaux générés :

- `merchant_report`
- `cohort_retention`
- `cohort_revenue`
- `cohort_best_3_months`

Le rapport de qualité est généré dans `output/data_quality_report/`.

## 10. Équipe et attribution

Le projet est réalisé par trois membres :

- Mamadou Niang : structure, build, modèles, ingestion, validation, rapport qualité
  et configuration.
- Moussa Abdou Omar : transformations, fenêtrage temporel, détection des
  transactions suspectes.
- Aicha Ly : analytics, cohortes, optimisation Spark et application principale.

Le détail complet des rôles et contributions est disponible dans `CONTRIBUTIONS.md`
ainsi que dans `EQUIPE.md`.

## 11. Points de vigilance

- Sur Windows, l’exécution locale Spark peut échouer si `HADOOP_HOME` et `winutils`
  ne sont pas correctement configurés.
- Le cache et le broadcast peuvent être désactivés via `application.conf` avec les
  paramètres `enable-cache` et `enable-broadcast`.
- Les dépendances Spark restent en `Provided` pour un packaging propre et un
  déploiement compatible `spark-submit`.
