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

Les contributions du membre C sont à consigner par son auteur lors de
l'intégration de son lot.
