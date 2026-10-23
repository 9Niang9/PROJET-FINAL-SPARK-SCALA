# Contributions

## Membre A — Mamadou Niang

- Propriétaire des Parties 1, 2 et 7 : structure SBT et configuration du build,
  modèles, ingestion multi-format, validations, rapport de qualité et configuration
  externalisée.
- Le JAR est produit avec `sbt assembly` et destiné à `spark-submit`. Spark est
  une dépendance `Provided` afin de ne pas embarquer le runtime Spark dans le JAR
  de l'application.
- Historique Git : à compléter au fur et à mesure des commits effectués par Mamadou
  Niang dans le dépôt de groupe.

## Membre B — Moussa Abdou Omar

- Propriétaire de la Partie 3 : UDF `extractTimeFeatures`, `enrichTransactionData`,
  fonctions de fenêtrage et détection de transactions suspectes (bonus 3.4).
- Fichiers : `TimeFeatures.scala`, `DataTransformation.scala`,
  `TransformationApp.scala` et leurs tests (`TimeFeaturesSpec`,
  `DataTransformationSpec`).
- Historique Git : 5 commits sur le dépôt de groupe, un par fichier livré.

### Décisions techniques

- **Jointures** : `left join` depuis les transactions pour `users`, `products` et
  `merchants`. Les transactions dont le client, le produit ou le marchand est
  inconnu sont conservées avec des colonnes jointes nulles ; un `inner join`
  supprimerait des ventes réelles.
- **Valeurs aberrantes** : les tables de référence sont jointes sans filtrer les
  lignes rejetées, mais les valeurs hors bornes (âge, revenu, prix, note,
  commission) sont remplacées par `null`, avec les seuils de `AppConfig`.
- **Broadcast** : la table `merchants` (600 lignes) est diffusée lors de la
  jointure quand `enable-broadcast` est actif.
- **UDF** : calcul en logique pure Scala avec `java.time` en résolution stricte.
  Un timestamp nul ou invalide donne `None`, sans faire échouer le job.
- **Tranche d'âge** : Jeune < 25, Adulte de 25 à 44, Âge Moyen de 45 à 64,
  Senior à partir de 65, Inconnu si l'âge est absent. L'âge de 25 ans n'est pas
  couvert par le sujet : il est classé Adulte.
- **Fenêtres** : le cumul sur 7 jours utilise `rangeBetween` sur des secondes.
  Les jours actifs utilisent `collect_set` puis `size`, car `countDistinct` n'est
  pas supporté en fenêtre. Le délai depuis l'achat précédent utilise `lag`.
- **Transactions suspectes** : `is_suspicious` vaut 1 si au moins 2 critères sur 4
  sont remplis (montant supérieur de plus de 300 % à la moyenne du client, nuit,
  moins de 5 minutes après la transaction précédente, paiement CRYPTO). Un
  critère inconnu compte comme non rempli.

### Difficultés rencontrées

- `countDistinct` interdit dans une fenêtre : remplacé par `collect_set` et `size`.
- Contrôle des références orphelines : une colonne pouvant être nulle dans la table
  d'origine (`product_name`) faussait le compte, remplacée par `product_category`.
- Java 17 et supérieur : erreurs d'accès aux classes internes du JDK, résolues par
  les options `--add-opens` pour les tests.

### Relectures

- Relecture de la Partie 2 (ingestion, validation, rapport de qualité), le
  22 octobre 2026, par Moussa Abdou Omar. Les 4 tests de `DataValidationSpec`
  passent avec la configuration de A.
  - Points validés : les valeurs nulles sont rejetées (`isNull` testé avant la
    comparaison), chaque ligne rejetée garde un motif par règle violée, les seuils
    viennent d'`AppConfig`, les champs numériques sont des `Option`.
  - Remarque 1 : le timestamp est validé uniquement sur sa longueur de 14
    caractères. Une valeur comme `20261345999999` est donc considérée valide ; la
    transformation la convertit en `null` ensuite.
  - Remarque 2 : `readDataset` exécute un `count()` à chaque lecture, puis le
    rapport recompte les mêmes données. Un `cache()` après la lecture éviterait de
    relire les fichiers plusieurs fois.
  - Remarque 3 : les tests couvrent un cas valide et un cas invalide par jeu de
    données. Un test avec une valeur nulle manquerait pour confirmer le rejet.
  - Remarque 4 : le rapport de qualité n'indique pas les références orphelines
    (bonus 2.5) et les lignes rejetées ne sont pas écrites sur disque, seulement
    disponibles en mémoire.
  - Remarque 5 : les valeurs par défaut sont dupliquées entre `application.conf`
    et `AppConfig`.
- Relecture de la Partie 4 (KPI marchands, cohortes) : à faire quand le lot C
  sera disponible dans le dépôt.

## Membre C — Aicha Ly

- Propriétaire des Parties 4, 5 et 6 : KPI marchands, cohortes, optimisations
  Spark et application principale.
- Fichiers : `Analytics.scala`, `SparkOptimizations.scala`, `MainApp.scala`,
  `SparkSessionBuilder.scala`, `StepTimer.scala`, `ResultsWriter.scala`.
- Bonus traités : taux de transactions suspectes par marchand (4.1), revenu par
  cohorte et par utilisateur (4.2), exécution par étape (6.2).
- Historique Git : 5 commits sur le dépôt de groupe.
- Charge de travail : plus de 8 heures.

### Décisions techniques

- **Marchands inconnus** : les transactions dont le marchand n'existe pas dans
  le référentiel sont exclues du rapport marchand, car on ne peut ni les nommer
  ni les classer.
- **Commission** : montant multiplié par `commission_rate`. Une commission hors
  bornes est déjà `null` depuis la Partie 3 et n'est donc pas comptée.
- **Cohortes** : `cohort_month` est le mois de la première transaction,
  `period_index` est calculé avec `months_between`, la rétention est exprimée en
  pourcentage de la taille initiale. En cas d'égalité à 3 mois, la cohorte la
  plus grande est retenue.
- **Cache** : `cache()` sur les jeux lus, `persist(MEMORY_AND_DISK_SER)` sur le
  résultat enrichi, volumineux, et `unpersist()` à la fin dans un bloc `finally`.
  Le tout se désactive avec `enable-cache`.
- **Broadcast** : appliqué aux petites tables (marchands, tailles de cohortes,
  ventes par tranche d'âge), désactivable avec `enable-broadcast`.
- **Sortie** : chaque résultat est écrit en CSV et en Parquet, en un seul
  fichier, car les agrégats sont petits.
- **Arrêt propre** : `MainApp` libère le cache et arrête la SparkSession dans un
  `finally`, puis renvoie un code de sortie 1 en cas d'erreur.

### Difficultés rencontrées

- `sbt run` s'arrêtait avec `NoClassDefFoundError: SparkSession`, car Spark est
  en portée `Provided`. Corrigé dans `build.sbt` avec une exécution en processus
  séparé et les options Java des tests.

### Relectures

- Relecture des Parties 1 et 7 (structure SBT, build.sbt, README, application.conf,
  AppConfig), le 23 octobre 2026, par Aicha Ly. `sbt clean compile` et `sbt test`
  passent sur mon poste (Java 21, sbt 1.10.7).
- Points validés : versions de Scala (2.12.18) et de Spark (3.5.5) compatibles,
    Spark en portée `Provided`, Typesafe Config intégré, plugin `sbt-assembly`
    avec une stratégie de fusion, case classes dans `com.ecommerce.models`,
    `.gitignore` conforme (`target/`, `project/target/`, `.idea/`), justification
    du choix de l'assembly dans `CONTRIBUTIONS.md`, aucun chemin ni seuil codé en
    dur, valeurs de repli dans `AppConfig`.
- Remarque 1 : la ligne `Compile / run / fullClasspath` du `build.sbt` n'avait
    pas d'effet, et `sbt run` échouait car Spark est en `Provided`. Corrigé dans le
    commit « Corrige sbt run pour les dépendances Spark en Provided ».
- Remarque 2 : les données sont dans `data/` à la racine du projet, alors que le
    sujet les place dans `src/main/resources/data/`. Le projet fonctionne, car
    `application.conf` pointe vers `data/...`, mais l'arborescence diffère du sujet.
- Remarque 3 : le README indique les versions requises, mais pas comment installer
    Scala, Spark et SBT, alors que la Question 1.3 le demande.
- Remarque 4 : les valeurs par défaut sont dupliquées entre `application.conf`
    et `AppConfig` : une modification doit être faite aux deux endroits.
- Remarque 5 : le dossier `utils/` ne contenait pas de `SparkSessionBuilder`,
    cité dans le sujet. Il est ajouté par le membre C : Aicha. `AppConfig` joue le rôle
    du `ConfigLoader`.
