# Lab PySpark silver : résultats et réponses

- Script : `lab_silver/lab_pyspark_silver.py` (cellules `# %%`, exécutable d'un bloc)
- Sortie complète : `lab_silver/output.txt`
- Exécution : `spark-submit --driver-memory 2g --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 lab_silver/lab_pyspark_silver.py > lab_silver/output.txt`
- Comparaison dbt / Spark : `lab_silver/compare_dbt_spark.sql` (résultat dans `lab_silver/compare_dbt_spark.txt`)
- Environnement : Spark 4.1.1 / Scala 2.13, `local[*]`, pod `vscode-pyspark` à 4 vCPU / 4000Mi, Iceberg 1.11.0 (runtime `4.1_2.13`, celui de la page `4.0` ne correspond pas à Spark 4.1)
- Métadonnées Iceberg versionnées : `warehouse/silver/{users,orders}/metadata/` (les fichiers `data/` ne sont pas commités)
- Nettoyage S3 non effectué (consigne de l'enseignante) : `bronze/`, `silver/` et `warehouse/` restent dans le bucket.

## Chiffres clés

| Mesure | Valeur |
|---|---|
| Bronze `users` / `orders` | 50 / 2 829 lignes |
| Doublons simulés puis supprimés (`row_number()`) | 2 832 -> 2 829 |
| DataFrame API vs Spark SQL (`stg_orders`) | identiques (asserts OK) |
| `city` NULL après parsing | 6 (états `AP`, `AE`, `AA`), 37 états distincts |
| `state` / `zip_code` vides | 0 / 0 |
| Part-files Parquet `users` / `orders` | 1 / 1 (+ `_SUCCESS`) |
| Taille `orders` Parquet (snappy) / Iceberg (zstd) | 139 556 B / 84 055 B |
| Temps de build silver (transfo + écriture Parquet) | 17,8 s (démarrage session 5,9 s, script total 73 s) |
| Exercice 1 : `birthdate_is_valid = false` | 5 / 50 (DataFrame = SQL = dbt) |
| Exercice 2 : `regexp_extract` / UDF Python / `pandas_udf` | 44,9 / 107,2 / 109,3 ms |
| Exercice 3 : sans cache / avec cache | 672-834 ms / 444-489 ms |
| Exercice 4 : Iceberg courant / `VERSION AS OF` snapshot précédent | 2 830 / 2 829 |
| DuckDB : lignes dbt vs Spark (orders, users) | 2 829 = 2 829, 50 = 50 |
| DuckDB : `sum(quantity)` dbt vs Spark | 8 615 = 8 615 |
| DuckDB : lignes `orders` présentes d'un seul côté (`EXCEPT` bidirectionnel) | 0 |

## SparkSession et accès S3

**Les credentials sont temporaires : que devient un job Spark si le service redémarre en plein lab ?**
- En `local[*]`, le driver et les executors vivent dans le pod. Si le pod redémarre, le job meurt avec lui : DataFrames en mémoire, cache et Spark UI sont perdus, il faut tout relancer.
- Vécu pendant ce lab : sur un pod à 2 Go, le conteneur a été tué (OOM) pendant l'exercice 3. Relancer avec 4 vCPU / 4000Mi a réglé le problème.
- Au redémarrage, Onyxia injecte de **nouvelles** variables `AWS_*`. Les clés codées en dur ou copiées dans `~/.aws/credentials` deviennent obsolètes. Le token STS expire aussi de lui-même (ici `max_dur = 43200 s`, soit 12 h) : un job long qui le dépasse échoue en `403 ExpiredToken`.
- Côté données : une écriture Parquet interrompue laisse des part-files sans `_SUCCESS`, donc un répertoire incohérent. Une écriture Iceberg interrompue ne commite pas de snapshot : la table reste dans son état précédent, avec au pire des fichiers orphelins.

**Pourquoi `SimpleAWSCredentialsProvider` échoue ici ?**
- Il ne lit que `access.key` et `secret.key` et ignore `fs.s3a.session.token`.
- Une clé STS (`ASIA...`) n'est valide qu'accompagnée de son token (en-tête `x-amz-security-token`). Sans ce token, S3 rejette la requête (`InvalidAccessKeyId` / `403`).
- `TemporaryAWSCredentialsProvider` envoie les trois éléments. Une clé permanente (`AKIA...`) n'a pas de token, c'est pour cela que le provider simple fonctionnait auparavant.
- Même famille de piège vu en lançant le lab : les variables `AWS_*` périmées d'Onyxia prennent le pas sur `~/.aws/credentials`, d'où un `InvalidAccessKeyId` alors que le fichier est correct.

## Couche silver

**Lecture de la requête état / utilisateurs**
- 37 états distincts pour 50 utilisateurs : chaque état compte 1 à 3 utilisateurs, le jeu Faker est uniformément dispersé.
- `missing_city` ne concerne que `AE`, `AP` et `AA` (6 utilisateurs). Ce sont des adresses militaires américaines (`Unit 6184 Box 9593` / `DPO AP 09617`) : pas de virgule ni de ville, seulement `APO/FPO/DPO` + code militaire + zip.
- La regex `^(.+), [A-Z]{2} [0-9]{5}$` ne matche pas, `regexp_extract()` renvoie `""`, que le `when(... == "", None)` convertit en NULL. Ce n'est pas une erreur de parsing mais un format d'adresse différent.

**DataFrame API vs Spark SQL**

| Critère | DataFrame API | Spark SQL |
|---|---|---|
| Exploration ponctuelle | Bien : chaînage, autocomplétion, `show()` à chaque étape | Bien aussi, mais moins modulaire |
| Modèle réutilisé par des collègues | Bon si l'équipe code en Python (fonctions, tests unitaires, factorisation) | Lisible par tous, y compris les analystes SQL, portable vers dbt ou DuckDB |
| Erreurs | Détectées à la construction (noms de colonnes, types Python) | Détectées au parsing de la chaîne SQL |
| Performance | Identique : même plan Catalyst (vérifié avec `explain()`, Window + `WindowGroupLimit` dans les deux cas) | Identique |

- Recommandation : DataFrame API pour l'exploration et la logique paramétrée, SQL pour un modèle partagé et revu par une équipe mixte.

**Où d'autre `""` au lieu de `NULL` fausse silencieusement les résultats ?**
- `count(col)`, `avg`, `min` et `max` comptent `""` comme une valeur. Le taux de remplissage paraît bon alors que la donnée manque.
- `coalesce(col, 'défaut')` ne remplace pas `""`.
- Jointures : `""` matche `""` alors que `NULL` ne matche jamais, ce qui crée de fausses correspondances (par exemple tous les zip vides joints ensemble).
- `GROUP BY` crée un groupe `""` distinct du groupe NULL. Les tests `not_null` de dbt passent alors que la donnée est vide.
- Casts : `CAST('' AS INT)` donne NULL ou une erreur selon le mode ANSI. Le comportement change selon la configuration.
- Ici `state` et `zip_code` n'ont aucune chaîne vide (vérifié), mais seul `city` est protégé par le code.

## Parquet

**Combien de part-files sous `silver/spark/users.parquet/` et pourquoi ?**
- Un seul `part-00000-...snappy.parquet` (+ `_SUCCESS`), et de même pour `orders.parquet`.
- Spark écrit **un fichier par partition** du DataFrame final. `users` vient d'un seul petit CSV (7 Ko), lu en une seule partition. `orders` passe par un shuffle (fenêtre `row_number`, 200 partitions par défaut), mais l'Adaptive Query Execution (AQE) fusionne les partitions vides ou minuscules après le shuffle, ce qui donne 1 partition (vérifié : `stg_users partitions: 1 | stg_orders partitions: 1`).
- Aucun `coalesce(1)` n'est fait dans le script : c'est AQE qui donne ce résultat.

**Que perd-on avec `coalesce(1)` avant l'écriture sur un gros volume ?**
- Le parallélisme : une seule tâche écrit tout, ce qui laisse un seul cœur actif et peut faire déborder la mémoire ou le disque de cet executor.
- `coalesce` ne déclenche pas de shuffle, il remonte le goulot d'étranglement dans les étapes amont : tout le calcul du stage final tourne aussi sur une seule tâche.
- Les lectures futures : un fichier géant se parallélise moins bien (et les petits fichiers coûtent cher, d'où la cible de 128 Mo à 1 Go par fichier).
- Alternative : `repartition(n)` dimensionné sur le volume, ou laisser AQE / Iceberg (`write.target-file-size-bytes`) gérer la taille des fichiers.

## Iceberg

**`silver/spark/orders.parquet/` vs `warehouse/silver/orders/` : données vs description**

| Fichier | Rôle |
|---|---|
| `orders.parquet/part-*.parquet` | Données. Le "schéma de table" se réduit au footer Parquet et au listing du répertoire. |
| `orders.parquet/_SUCCESS` | Marqueur de fin de job, pas une garantie transactionnelle |
| `warehouse/.../data/*.parquet` | Données (zstd, 84 Ko contre 140 Ko en snappy) |
| `metadata/vN.metadata.json` | Description de la table : schéma, partitionnement, propriétés, liste des snapshots, snapshot courant |
| `metadata/snap-*.avro` (manifest list) | Liste des manifests d'un snapshot |
| `metadata/*-m0.avro` (manifest) | Liste des fichiers de données avec stats (nb de lignes, min/max par colonne) |
| `metadata/version-hint.text` | Pointeur vers la version courante du `metadata.json` (catalogue Hadoop) |

**Lecture pendant un overwrite du répertoire Parquet, et avec Iceberg ?**
- Parquet : l'overwrite supprime puis réécrit les fichiers. Un lecteur peut lister un répertoire vide ou mélanger anciens et nouveaux fichiers, voire échouer (`FileNotFoundException`). Il n'y a pas d'isolation.
- Iceberg : le lecteur résout un snapshot au début de la requête et lit exactement ses fichiers. L'écrivain produit de nouveaux fichiers puis bascule le pointeur de métadonnées de façon atomique. Le lecteur voit l'ancienne version complète ou la nouvelle, jamais un mélange (isolation de type snapshot).
- Démo de l'exercice 4 : après l'append Parquet, l'ancienne version a disparu et `VERSION AS OF` échoue (`UNSUPPORTED_FEATURE.TIME_TRAVEL`). Iceberg, lui, renvoie toujours 2 829 lignes sur le snapshot précédent.

**Deux jobs qui commitent en même temps avec un pointeur fichier**
- Le catalogue Hadoop repose sur la création ou le rename de `vN+1.metadata.json` et la réécriture de `version-hint.text`. Sur S3, il n'y a pas de rename atomique, et le put-if-absent n'est pas garanti selon le stockage. Deux jobs peuvent écrire le même `vN+1` : le dernier écrase l'autre, et le commit perdu disparaît sans erreur (lost update). Le `version-hint` peut aussi pointer vers une version incohérente.
- Un catalogue service (REST/Polaris, Nessie, Glue, Hive Metastore, JDBC) fait un compare-and-swap atomique du pointeur. Le second commit détecte le conflit, relit l'état, ré-applique ses changements (retry optimiste) ou échoue proprement. S'y ajoutent les droits d'accès, le multi-moteur et le nommage centralisé des tables.

## Comparaison avec dbt

**Différences de schéma (UUID) et impact pour les consommateurs**

| Colonne | dbt (DuckDB) | Spark |
|---|---|---|
| `order_id`, `user_id` | `UUID` | `VARCHAR` |
| `ordered_at` | `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP` (sans fuseau, session UTC) |
| `username_normalized`, `birthdate_is_valid` | présentes | absentes (`birthdate_is_valid` exportée à part dans `users_birthdate_check.parquet`) |

- Spark n'a pas de type UUID : il écrit des chaînes de 36 caractères. DuckDB écrit un `UUID` Parquet (logical type, 16 octets). C'est plus compact et validé, mais tous les lecteurs ne le gèrent pas de la même façon.
- Conséquences : une jointure entre une table dbt et une table Spark impose un cast (`::VARCHAR` ou `::UUID`), sans quoi on obtient une erreur de type ou une comparaison ratée. Les timestamps avec et sans fuseau peuvent décaler les heures si la session n'est pas en UTC. Un consommateur qui lit les deux sorties doit donc connaître le moteur producteur.
- Les **valeurs**, elles, sont identiques : 0 ligne de différence sur `orders` après cast commun, mêmes volumes et même `sum(quantity)`.

**Un objet par modèle vs un répertoire de part-files**
- Plus simple à consommer : un fichier unique (`stg_orders.parquet`), ouvrable par n'importe quel outil sans glob.
- Plus scalable : le répertoire de part-files, écrit en parallèle par N tâches et lu en parallèle, sans fichier géant. Au-delà, une table Iceberg combine les deux : un seul nom logique, N fichiers physiques.

**Garanties des tests dbt vs ce lab**
- dbt : tests déclaratifs exécutés à chaque `dbt build` (`unique`, `not_null`, `accepted_values`, `relationships`, tests singuliers SQL). Un échec bloque les modèles en aval.
- Ce lab : des `assert` Python ponctuels (nombre de lignes, égalité DataFrame/SQL, relecture Parquet/Iceberg) et la comparaison DuckDB. Pas de blocage du DAG, pas de rapport, pas d'historique.
- Pour être à parité, il faudrait écrire : des fonctions de test réutilisables (unicité de `order_id`, non-nullité, valeurs acceptées pour `product`, intégrité référentielle `orders.user_id` vers `users.user_id`), intégrées à `pytest` ou à un framework de qualité (Great Expectations, Deequ/PyDeequ, `chispa` pour comparer des DataFrames) et exécutées en CI avant publication.

**Équivalents de `ref()` et `source()`**
- `source()` : les chemins `s3a://.../bronze/*.csv` codés en dur et lus par `spark.read`.
- `ref()` : les variables Python (`stg_users`, `stg_orders`) et les vues temporaires (`createOrReplaceTempView`) réutilisées par les cellules suivantes, puis les tables `lab.silver.*` du catalogue Iceberg.
- La différence : dbt dérive le graphe de dépendances (DAG), l'ordre d'exécution et la documentation de ces appels. Dans le notebook, l'ordre des cellules **est** le DAG, implicite et fragile si on exécute les cellules dans le désordre.

**Pourquoi les temps de build ne sont pas comparables**
- Spark paie un coût fixe : démarrage de la JVM et de la session (5,9 s), téléchargement ou résolution du jar Iceberg, planification Catalyst et ordonnancement des stages. Sur 3 000 lignes, ce surcoût domine.
- L'évaluation est paresseuse : une cellule de transformation ne fait rien tant qu'il n'y a pas d'action. Les temps par cellule dépendent donc de l'endroit où tombe l'action (`count`, `show`, `write`), et du cache.
- `dbt build` inclut les tests et la compilation, et DuckDB est un moteur in-process sans coût de cluster.
- Il faudrait mesurer le même périmètre (lecture, transformation, écriture, tests), à froid et à chaud, sur plusieurs runs et à plusieurs volumes.

**dbt + DuckDB ou Spark ?**

| Contexte | Choix | Pourquoi |
|---|---|---|
| Batch quotidien de 10 Go | dbt + DuckDB | Tient sur une machine (DuckDB gère le hors-mémoire). Simple à opérer, testé, peu coûteux. Spark serait surdimensionné. |
| Batch quotidien de 10 To | Spark (dbt-spark possible) | Il faut distribuer le calcul et les I/O. Une seule machine ne suit pas, même avec DuckDB. |
| Équipe d'analystes 100 % SQL | dbt (moteur selon le volume) | Modèles SQL, `ref()`, tests et documentation. L'équipe reste autonome. dbt peut cibler Spark ou un entrepôt si le volume grandit. |

## Exercices

**1. `birthdate_is_valid` (jointure users / première commande)**
- Règle : `coalesce(birthdate <= to_date(first_ordered_at), true)`. Un utilisateur sans commande est considéré comme valide.
- DataFrame et SQL : 0 ligne de différence. 45 valides, 5 invalides (naissances en 2022-2026 pour des premières commandes en 2020, artefact de Faker). Même résultat côté dbt (5 invalides).

**2. UDF Python vs `pandas_udf` vs `regexp_extract()` (zip code)**
- `regexp_extract` : 44,9 ms. UDF Python : 107,2 ms. `pandas_udf` : 109,3 ms. Les trois donnent le même résultat.
- `regexp_extract` est natif (JVM, Catalyst, sans sérialisation) : c'est toujours le premier choix.
- Ici, sur 50 lignes, `pandas_udf` n'apporte rien car le coût fixe d'Arrow et du worker Python domine. Sur de gros volumes, il devient nettement plus rapide que l'UDF ligne à ligne grâce au traitement vectorisé par lots, mais il reste derrière une fonction native.

**3. Cache des DataFrames bronze**
- Sans cache, chaque action relit les CSV sur S3 : les stages montrent `input = 7 351 B (50 rec)` et `331 568 B (2 829 rec)`.
- Avec cache : `input = 9 416 B` et `147 856 B`, avec 1 "rec" par partition, ce qui correspond au bloc en mémoire et non à des lignes CSV. Il n'y a plus de lecture S3 ni de parsing CSV. L'onglet Storage montre 1 partition cachée par DataFrame.
- Temps : 672-834 ms sans cache contre 444-489 ms avec. Le gain est modeste à cette échelle, mais il compte quand les données bronze sont relues par plusieurs modèles. Penser à `unpersist()`.
- Les stages après le shuffle (`shuffleRead`) restent identiques : le cache n'évite que la lecture et le parsing du bronze, pas la fenêtre de déduplication.

**4. Append + time travel Iceberg vs répertoire Parquet**
- Iceberg : l'append crée un snapshot `append` dont le parent est le snapshot `overwrite`. Résultat : 2 830 lignes en courant, 2 829 avec `VERSION AS OF <snapshot précédent>`. La table `history` montre l'ascendance. Le snapshot du 7 octobre (run précédent) reste dans l'historique, mais avec `is_current_ancestor = false` car `createOrReplace` a démarré une nouvelle lignée.
- Parquet : l'append ajoute des fichiers (4 objets). Il n'y a ni snapshot ni historique, l'état précédent est perdu et `VERSION AS OF` échoue.
- Note : la table `lab.silver.orders` contient donc 2 830 lignes après l'exercice, alors que la sortie Parquet `silver/spark/orders.parquet` en a 2 829. La comparaison dbt porte sur cette dernière.
