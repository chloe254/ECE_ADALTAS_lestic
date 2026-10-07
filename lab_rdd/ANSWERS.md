# Lab PySpark RDD : résultats et réponses

- Script : `lab_rdd/lab_pyspark_rdd.py` (cellules `# %%`, exécutable d'un bloc)
- Sortie complète : `lab_rdd/output.txt`
- Exécution : `spark-submit --driver-memory 768m lab_rdd/lab_pyspark_rdd.py > lab_rdd/output.txt`
- Environnement : Spark 4.1.1, `local[*]`, pod à 2 vCPU (`defaultParallelism = 2`, `defaultMinPartitions = 2`)
- Les tailles de shuffle "Spark UI" sont lues par le script via l'API REST de l'UI (`/api/v1/.../stages`), mêmes chiffres que l'onglet Stages de `http://localhost:4040`.

## Chiffres clés

| Mesure | Valeur |
|---|---|
| Partitions `users_lines` / `orders_lines` | 2 / 2 |
| `repartition(8)` / `coalesce(1)` | 8 / 1 |
| Lignes `orders` (hors en-tête) | 2 829 |
| `users_lines.count()` (lignes physiques) | 101 |
| `users_records.count()` (filtre UUID) | 50 (= `spark.read.csv(multiLine=True)`) |
| Shuffle write `reduceByKey` / `groupByKey` | 2 513 B / 6 230 B (x2,5) |
| `orders_rdd` / `orders_dedup` | 2 829 / 2 829 (aucun doublon d'`order_id` dans ce bronze) |
| `join` / `leftOuterJoin` | 2 829 / 2 829, 0 commande orpheline |
| Join co-partitionné (`partitionBy(4)` des 2 côtés) | 1 seul stage, 0 B de shuffle |

## RDD creation and partitions

**Qu'est-ce qui décide du nombre de partitions de `textFile()` ?**
- `textFile(path, minPartitions)` délègue au `TextInputFormat` de Hadoop. `minPartitions` vaut par défaut `sc.defaultMinPartitions = min(defaultParallelism, 2)`, donc 2 ici.
- Hadoop calcule `splitSize = max(minSize, min(totalSize / minPartitions, blockSize))`. Les fichiers (7 Ko et 330 Ko) sont minuscules face à la taille de bloc locale (32 Mo) : c'est `minPartitions` qui l'emporte, d'où 2 partitions.
- Sur un gros fichier, c'est la taille de bloc (32 Mo en local, 128 Mo sur HDFS) qui dicte le nombre de partitions.

**`repartition()` vs `coalesce()` ?**
- `repartition(n)` = `coalesce(n, shuffle=True)` : il **shuffle** toujours. Visible dans le lineage : `ShuffledRDD` et un `+-(2)` (frontière de stage).
- `coalesce(n)` sans shuffle fusionne des partitions existantes localement (`CoalescedRDD` directement au-dessus du fichier, aucune frontière de stage). Il ne sert qu'à **réduire** le nombre de partitions ; pour augmenter, il faut le shuffle.

## Lazy evaluation and lineage

**Deuxième `count()` : nouveau stage ?**
- Oui. Les deux `count()` ont créé chacun un job et un stage (stages 1 et 2, 2 tâches chacun).
- Sans cache, une RDD n'est qu'une recette (le lineage) : chaque action relit `orders.csv` et réapplique le `filter`.

**Où `.cache()` changerait la réponse ?**
- Sur `orders_data`, **avant** le premier `count()` : `orders_data = orders_lines.filter(...).cache()`.
- Le premier `count()` matérialise les partitions en mémoire, le second les lit depuis le cache. Il y a toujours un job/stage (une action en lance forcément un), mais il ne relit plus le fichier : le lineage affiche `CachedPartitions: 2`.
- `.cache()` placé après le premier `count()` ne sert qu'aux actions suivantes, et c'est la première action qui suit qui paie le coût du remplissage.

## The cost of no schema: the multiline address

- `take(6)` montre bien une ligne sur deux qui commence par `Robinsonshire, KY 01352",...` : la fin de l'adresse.
- 101 lignes = 1 en-tête + 50 utilisateurs x 2 lignes physiques. Après le filtre UUID : 50.

**Pourquoi `spark.read.csv(multiLine=True)` n'a pas ce problème ?**
- Le lecteur CSV comprend la syntaxe CSV : un saut de ligne **entre guillemets** fait partie du champ. Avec `multiLine=True`, il parse le fichier comme un flux d'enregistrements, pas de lignes.
- Contrepartie : un fichier `multiLine` n'est plus découpable (splittable) n'importe où, car on ne sait pas si un `\n` ouvre un enregistrement sans lire depuis le début. Le fichier est lu par une seule tâche.
- `textFile()` ne connaît que `\n` : il ignore totalement les guillemets.

**Condition pour qu'un lecteur ligne à ligne soit sûr ?**
- Un enregistrement = une ligne : le séparateur d'enregistrement (`\n`) ne doit jamais apparaître dans les données, ou être échappé (`\n` littéral).
- C'est le principe du JSON Lines / NDJSON, des logs, du TSV sans retours à la ligne. Cela rend aussi le fichier découpable à n'importe quel octet (on se resynchronise au prochain `\n`).

## Manual parsing of orders

**Virgule en trop dans `product` ?**
- `line.split(",")` renvoie 6 éléments, le dépaquetage en 5 variables lève `ValueError: too many values to unpack (expected 5)` (testé dans le script).
- L'erreur ne survient pas à la définition du `map` (lazy) mais à la première **action** qui touche cette ligne, en faisant échouer la tâche puis le job (après 4 tentatives sur un cluster).
- Même un split naïf "sans erreur" serait faux sur un champ entre guillemets du type `"bread, white"`. La bonne pratique : le module `csv` de Python dans un `mapPartitions`, ou mieux, le lecteur CSV de Spark.

**Où le DataFrame API fait-il ce contrôle, et quand ?**
- À la lecture, dans le parser CSV, selon un schéma (fourni ou inféré avec `inferSchema`) et le `mode` : `PERMISSIVE` (défaut, champs invalides à `null`, ligne brute éventuellement dans `columnNameOfCorruptRecord`), `DROPMALFORMED` ou `FAILFAST`.
- C'est aussi exécuté au moment d'une action (lecture lazy), mais le contrôle est centralisé, typé et configurable, au lieu d'un crash Python au milieu du job. `inferSchema=True` déclenche même une passe de lecture immédiate.

## Key-value RDDs: reduceByKey vs groupByKey

| | Shuffle write (stage map) | Shuffle read (`take(5)`, 1 partition) |
|---|---|---|
| `reduceByKey(add)` | 2 513 B | 1 065 B |
| `groupByKey().mapValues(sum)` | 6 230 B | 2 643 B |

**Que shuffle `groupByKey`, et pourquoi c'est plus cher ?**
- `reduceByKey` fait une agrégation **côté map** (combiner) : chaque partition envoie une seule paire `(user_id, somme partielle)` par clé, soit au plus 50 x 2 paires ici.
- `groupByKey` envoie **toutes les valeurs** : les 2 829 quantités traversent le réseau, puis sont regroupées en listes côté reduce.
- Coût : plus d'octets sérialisés, écrits sur disque, transférés ; et côté reduce toute la liste d'une clé doit tenir en mémoire (risque d'OOM sur une clé très fréquente, le fameux skew).
- Ici x2,5 seulement car le jeu est minuscule ; l'écart croît avec le nombre de valeurs par clé.

**Quand `groupByKey` est inévitable ?**
- Quand l'agrégat a besoin de **toutes** les valeurs à la fois et n'est pas décomposable en fonction associative et commutative : médiane, percentiles exacts, tri des événements d'un utilisateur (sessionisation), construction de la séquence complète, top-N avec contexte complet.
- Nuance : beaucoup de cas "groupés" restent exprimables avec `aggregateByKey`/`combineByKey` (moyenne, comptage distinct via un `set`, top-N borné via un tas). `groupByKey` est le dernier recours.

## Manual deduplication

- Résultat : 2 829 avant comme après ; une vérification `reduceByKey` sur `(order_id, 1)` confirme qu'aucun `order_id` n'apparaît deux fois dans ce bronze. La logique est néanmoins correcte. À noter : la colonne s'appelle `date` ici (pas `ordered_at`), et la comparaison de chaînes ISO 8601 de même format équivaut à une comparaison chronologique.

**Qui joue le rôle du `PARTITION BY` ?**
- Le **shuffle de `reduceByKey`**, avec le `HashPartitioner` sur la clé `order_id` : toutes les versions d'un même `order_id` arrivent dans la même partition, où `most_recent` les réduit.
- Différence notable : `reduceByKey` combine aussi **avant** le shuffle (côté map), alors que la window function shuffle toutes les lignes puis trie chaque partition. Pour garder une seule ligne, `reduceByKey` est donc plus efficace.

**Quelle version exprime le mieux l'intention ?**
- La window function : `row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) = 1` dit "garde la plus récente par commande" de façon déclarative, lisible par n'importe quel analyste SQL, et Catalyst choisit l'exécution.
- La version `reduceByKey` encode le "comment" (une fonction binaire qui doit être associative et commutative, sinon résultat non déterministe ; ici les égalités de date prennent arbitrairement `order_b`).
- En pratique côté DataFrame, `dropDuplicates` ou `max_by`/`groupBy().agg(max_by(...))` sont encore plus simples.

## Joining orders with users

- `enriched` : 2 829 éléments `(user_id, (order, username))`, par exemple `('f143262f…', ({…'product': 'bread'}, 'kayla51'))`.
- Le lineage du join montre un `UnionRDD` des deux côtés puis un `ShuffledRDD` : PySpark implémente `join` comme un `cogroup` (union taguée + `partitionBy`), donc les **deux** RDD sont shufflées.

**Condition pour éviter le shuffle ?**
- Que les deux RDD soient **co-partitionnées** : même `Partitioner` (même type, même nombre de partitions), déjà appliqué, et de préférence **persisté** (sinon le `partitionBy` est recalculé à chaque action).
- Démontré dans le script : `users_kv.partitionBy(4).cache()` et `orders_kv.partitionBy(4).cache()`, puis `join` : **1 seul stage, 0 B de shuffle**. Le shuffle a été payé une fois, en amont, et est amorti si on joint plusieurs fois.
- Équivalent DataFrame : tables bucketisées sur la clé, ou broadcast join (petite table envoyée à chaque exécuteur, aucun shuffle de la grande). Avec 50 utilisateurs, le broadcast est la bonne réponse en vrai.

**`leftOuterJoin()` si un `user_id` n'a pas de correspondance ?**
- La commande est conservée, avec `None` à la place du username : `(user_id, (order, None))`. Avec `join` (inner), elle disparaîtrait silencieusement.
- Ici : 0 orpheline (2 829 dans les deux cas), cf. exercice 2 pour un cas provoqué.

**Risque de `.collect()` sur `enriched` ?**
- `collect()` rapatrie **toute** la RDD dans la mémoire du driver (un seul process). Sur un gros volume : `OutOfMemoryError` du driver ou dépassement de `spark.driver.maxResultSize`, et perte de tout le parallélisme.
- Un join peut aussi **multiplier** les lignes (clé dupliquée des deux côtés = produit cartésien par clé). Préférer `take`, `count`, une agrégation avant, ou une écriture distribuée (`saveAsTextFile`, DataFrame `write`).

## Exercices

### 1. `aggregateByKey` -> `(order_count, total_quantity)`

```python
user_stats = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).aggregateByKey(
    (0, 0),
    lambda acc, q: (acc[0] + 1, acc[1] + q),    # seqOp, dans une partition
    lambda a, b: (a[0] + b[0], a[1] + b[1]),    # combOp, entre partitions
)
user_avg = user_stats.mapValues(lambda ct: ct[1] / ct[0])
```

- Exemple : `f143262f…` -> `(4, 12)`, moyenne 3,0 ; `ce9e1a11…` -> `(85, 253)`, moyenne 2,976.
- Totaux identiques à `reduceByKey` (vérifié). Top 3 des moyennes : 3,571 / 3,478 / 3,389.
- Pourquoi `aggregateByKey` : le type de l'accumulateur `(count, total)` diffère du type des valeurs (`int`), ce que `reduceByKey` ne permet pas directement. Une seule passe, avec combinaison côté map.

### 2. `user_id` fictif + `leftOuterJoin`

- Ajout d'une commande `user_id = 00000000-0000-0000-0000-000000000000` via `union(sc.parallelize([...]))`.
- `leftOuterJoin(users_kv).filter(lambda kv: kv[1][1] is None).count()` -> **1**, la commande fictive avec `None` comme username. C'est exactement le contrôle d'intégrité référentielle qu'on ferait en silver.

### 3. Équivalent DataFrame

```python
spark.read.csv("orders.csv", header=True, inferSchema=True).groupBy("user_uuid").sum("quantity")
```

- 2 lignes contre ~15 (parse manuel + `map` + `reduceByKey`), résultat identique (vérifié).
- `.explain()` : `HashAggregate(partial_sum)` -> `Exchange hashpartitioning(user_uuid, 200)` -> `HashAggregate(sum)`, avec `FileScan csv` qui ne lit que `user_uuid` et `quantity` (`ReadSchema`, column pruning).
- Le `partial_sum` avant l'`Exchange` est exactement le combiner de `reduceByKey` : Catalyst l'ajoute automatiquement, sans risque d'écrire un `groupByKey` par erreur.
- `toDebugString()` montre des `PythonRDD` opaques : Spark ne voit que des lambdas Python, ne peut ni élaguer les colonnes ni optimiser, et sérialise chaque ligne entre JVM et Python. Le DataFrame reste entièrement dans la JVM (Tungsten).
- Le `200` vient de `spark.sql.shuffle.partitions` ; l'AQE (`AdaptiveSparkPlan`) le réduit à l'exécution.

### 4. Cache vs pas de cache (ms, `time.perf_counter()`)

| | `orders.count` 1er appel | `orders.count` médiane de 5 | `user_quantity.count` médiane de 5 | Onglet Storage |
|---|---|---|---|---|
| Sans `.cache()` | 130 | 205 | 156 | vide |
| Avec `.cache()` | 288 | 126 | 167 | `orders_cached` : 2/2 partitions, 168 Ko en mémoire |

- **Avec cache**, le 1er appel est plus lent (il remplit le cache), les suivants ~40 % plus rapides : plus de relecture du fichier ni de re-parsing Python.
- **`user_quantity.count` ne change pas** : après sa première exécution, Spark **réutilise les fichiers de shuffle** déjà écrits et saute le stage map (stage "skipped" dans l'UI). Le cache amont devient inutile pour ce job.
- Sur 330 Ko, l'overhead fixe (planification, démarrage des workers Python) domine : les écarts sont de quelques dizaines de ms et bruités. Le gain du cache devient massif sur un vrai volume ou un parsing coûteux. Moralité : cacher ce qui est réutilisé plusieurs fois, et `unpersist()` ensuite.

## Ce que le DataFrame API apporte "gratuitement"

| Problème rencontré en RDD | Réponse DataFrame / SQL |
|---|---|
| Pas d'en-tête, pas de colonnes, pas de types | `header`, schéma, `inferSchema` |
| CSV multi-lignes cassé | `multiLine=True` |
| Lignes malformées = crash du job | `mode` PERMISSIVE / DROPMALFORMED / FAILFAST |
| Risque `groupByKey` | `partial_*` automatique avant l'`Exchange` |
| Dédoublonnage par fonction binaire | window functions, `dropDuplicates` |
| Join toujours shufflé | broadcast join automatique, bucketing, AQE |
| Lambdas opaques, sérialisation Python | Catalyst + Tungsten, column pruning, predicate pushdown |
