# EcommerceAnalytics

Projet d'analyse e-commerce distribué avec Scala, SBT et Apache Spark. Les fichiers
fournis sont conservés dans `data/`; leurs chemins et les paramètres de validation
sont configurés dans `src/main/resources/application.conf`.

## Prérequis

- Java 17
- Scala 2.12.18
- SBT 1.10.7
- Spark 3.5.5 pour lancer l'application avec `spark-submit`

Les dépendances Spark sont en portée `Provided` : elles sont nécessaires à la
compilation, mais le cluster Spark les fournit à l'exécution.

## Compilation et JAR

À la racine de ce projet :

```sh
sbt clean compile
sbt test
sbt assembly
```

Le JAR `target/scala-2.12/ecommerce-analytics-assembly.jar` contient l'application
et ses dépendances non-Spark. Spark reste en portée `Provided` et est fourni par
`spark-submit`. Le point d’entrée est `com.ecommerce.analytics.MainApp`.

## Exécution locale

Lancement local avec SBT :

```sh
sbt "run"
```

Le mode Spark local par défaut est `local[*]`; il peut être modifié dans
`application.conf`.

## Déploiement Spark

Après création du JAR avec `sbt assembly` :

```sh
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master local[*] \
  target/scala-2.12/ecommerce-analytics-assembly.jar
```

Remplacez `local[*]` par la cible du cluster pour un déploiement distribué.

## Fichiers de données et rapport qualité

Les entrées attendues sont `data/transactions.csv`, `data/users.json`,
`data/products.parquet` et `data/merchants.csv`. Le rapport qualité est écrit en
CSV sous `output/data_quality_report/` (un répertoire Spark contenant les parties
CSV et le marqueur de succès).

Les méthodes d'ingestion, de validation et de génération du rapport se trouvent
dans `com.ecommerce.analytics`. Les lignes rejetées restent disponibles avec la
colonne `rejection_reason`.

## Configuration

Les chemins de données, seuils de validation, paramètres Spark et options
d'optimisation se trouvent dans `src/main/resources/application.conf`. Des valeurs
de repli sont également définies dans `AppConfig` afin de permettre le chargement
si une clé manque.

## Équipe et attribution

Le projet est réalisé par trois membres : Mamadou Niang (membre A : structure,
build, modèles, ingestion, validation, rapport qualité et configuration),
Moussa Abdou Omar (membre B : transformations et fenêtres) et Aicha Ly
(membre C : analytique, optimisations Spark et application principale).
Le détail des rôles, des coordonnées et des noms Git figure dans `EQUIPE.md`.

## Étapes du pipeline et résultats

L'application accepte une étape en argument : `ingestion`, `transformation`,
`analytics` ou `all` (par défaut). Un argument inconnu affiche l'aide.
Exemple : `sbt "run transformation"`.

Les résultats sont écrits dans `output/csv/` et `output/parquet/` :
`merchant_report`, `cohort_retention`, `cohort_revenue` et
`cohort_best_3_months`. Le rapport de qualité est dans
`output/data_quality_report/`. Le cache et le broadcast se désactivent dans
`application.conf` (`enable-cache`, `enable-broadcast`).
