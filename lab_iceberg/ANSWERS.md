# Lab Iceberg : résultats et réponses

- Script : `lab_iceberg/lab_iceberg.py` (cellules `# %%`, exécutable d'un bloc)
- Sortie complète : `lab_iceberg/output.txt`
- Exécution : `spark-submit --driver-memory 2g --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 lab_iceberg/lab_iceberg.py > lab_iceberg/output.txt` (49,8 s)
- Environnement : Spark 4.1.1 / Scala 2.13, `local[*]`, pod `vscode-pyspark` 4 vCPU / 4000Mi. `hadoop-aws-3.4.2.jar` et le SDK AWS v2 (`bundle-2.29.52.jar`) sont déjà dans `$SPARK_HOME/jars`. Runtime Iceberg `iceberg-spark-runtime-4.1_2.13:1.11.0`. Catalogue `lab` de type `hadoop`, warehouse `s3a://user-c-lestic-ece/warehouse`.
- Entrées : `stg_orders` et `stg_users` relus depuis la sortie Parquet du lab précédent (`silver/spark/orders.parquet`, `silver/spark/users.parquet`).
- Point de départ : le lab précédent avait laissé 3 snapshots sur `lab.silver.orders` (dont l'append de son exercice 4). La table est recréée proprement (`DROP TABLE ... PURGE` puis `createOrReplace`) pour partir d'un seul snapshot, comme le suppose l'énoncé. Même chose pour `lab.silver.users`. Les métadonnées du lab précédent restent versionnées dans `warehouse/` du dépôt.
- Nettoyage S3 (`aws s3 rm --recursive warehouse/`) non effectué, à la demande de l'enseignante. Seul `spark.stop()` est exécuté.

## Écart avec les chiffres de l'énoncé

L'énoncé attend 5 000 commandes et `sum(quantity) = 14 976`. Notre bronze, généré au lab S3, compte **50 utilisateurs et 2 829 commandes** : on obtient donc **2 829 / 8 615**, puis 2 832 après l'append (au lieu de 5 003). Ces valeurs sont cohérentes avec les labs précédents, y compris avec la sortie dbt (2 829 lignes, 8 615 de quantité). Seule la taille du jeu de données diffère, le comportement d'Iceberg est le même.

## Chiffres clés

| Étape | Résultat |
|---|---|
| Création `lab.silver.orders` | 2 829 lignes, `sum(quantity)` = 8 615, 1 snapshot `overwrite`, 1 fichier de données (84 055 o) |
| Append de 3 lignes (`uuid()` comme `order_id`) | 2 snapshots, 2 832 lignes, 2 fichiers de données. Le fichier d'origine est toujours là |
| `VERSION AS OF` premier snapshot | 2 829 (lit 1 fichier) ; table courante : 2 832 (lit 2 fichiers) |
| `ADD COLUMN source` | 0 fichier de données réécrit, 0 nouveau snapshot, nouveau `v3.metadata.json` avec un schéma `schema-id = 1` |
| `INSERT` avec `source = 'spark'` | 3 snapshots, 2 833 lignes : `source` NULL pour 2 832 lignes, `'spark'` pour 1 |
| Fichiers de la table orders à la fin | 3 fichiers de données, 3 manifests, 3 manifest lists, 4 `vN.metadata.json`, 1 `version-hint.text` (136 882 o au total) |
| Exercices users | 50 lignes, puis 55 après l'append, puis 58 après l'ajout de `country` ; 3 snapshots, 2 schémas |

## Inspecter les fichiers

Après la création, `warehouse/silver/orders/` contient 5 objets :

| Objet | Taille | Rôle |
|---|---|---|
| `data/00000-7-...-00001.parquet` | 84 055 o | Les 2 829 lignes |
| `metadata/v1.metadata.json` | 1 933 o | Schéma, partitionnement, propriétés, liste des snapshots, snapshot courant |
| `metadata/snap-6232720141900360718-...avro` | 4 456 o | Manifest list du snapshot : liste des manifests |
| `metadata/60f6c886-...-m0.avro` | 7 347 o | Manifest : liste des fichiers de données, avec nombre de lignes et min/max par colonne |
| `metadata/version-hint.text` | 1 o | Numéro de la version courante (catalogue Hadoop) |

**1. Quels fichiers contiennent les lignes ?** Uniquement les fichiers Parquet de `data/`.

**2. Quels fichiers décrivent la table ?** Tout ce qui est dans `metadata/` : `vN.metadata.json` (la table), les manifest lists `snap-*.avro` (un par snapshot), les manifests `*-m0.avro` (les fichiers et leurs statistiques) et `version-hint.text` (le pointeur vers la version courante).

**3. Pourquoi une table Iceberg a-t-elle besoin de plus que ses fichiers Parquet ?**
- Un dossier de fichiers ne dit pas lesquels font partie de la table **maintenant**. Un fichier en cours d'écriture, un fichier d'un job planté ou un fichier d'une ancienne version se ressemblent tous.
- Les métadonnées donnent la liste exacte des fichiers d'une version. C'est ce qui permet le commit atomique, l'isolation des lecteurs, l'historique (time travel) et l'évolution de schéma.
- Elles évitent aussi de lister S3 (lent, et pas toujours cohérent), et les statistiques min/max des manifests permettent de sauter des fichiers entiers à la lecture.

## Tables de métadonnées

**4. Combien de snapshots ?** 1 : `6232720141900360718`, opération `overwrite`, `parent_id` NULL.

**5. Combien de fichiers de données dans la table courante ?** 1 (2 829 lignes, 84 055 o, format PARQUET).

**6. Qu'est-ce que la table `snapshots` contient que les fichiers Parquet n'ont pas ?**
- L'identité et la date de chaque version (`snapshot_id`, `committed_at`) et sa filiation (`parent_id`).
- L'opération (`overwrite`, `append`...) et le chemin de la manifest list.
- Un résumé du commit : `added-records = 2829`, `added-data-files = 1`, `total-records`, `total-files-size`, plus le moteur et l'application (`engine-name = spark`, `engine-version = 4.1.1`, `iceberg-version = 1.11.0`, `app-name = lab-silver-pyspark`).
- Un fichier Parquet ne connaît que son propre contenu. Il ne sait ni à quelle version de table il appartient, ni qui l'a écrit, ni quand.

## Second snapshot

| committed_at (UTC) | snapshot_id | parent_id | opération | lignes ajoutées | total | fichiers |
|---|---|---|---|---|---|---|
| 2026-10-12 12:58:54 | 6232720141900360718 | NULL | overwrite | 2 829 | 2 829 | 1 |
| 2026-10-12 12:59:00 | 5025655001167750826 | 6232720141900360718 | append | 3 | 2 832 | 2 |

**7. Combien de snapshots ?** 2. Le second a pour parent le premier.

**8. Les fichiers Parquet d'origine ont-ils disparu ?** Non. Le script compare la liste des fichiers de `data/` avant et après l'append : le fichier d'origine est toujours là (`original data files still on S3: True`) et un second fichier, petit, s'y ajoute (1 → 2). Le nouveau snapshot réutilise le manifest de l'ancien et ajoute un manifest pour le nouveau fichier.

**9. Pourquoi Iceberg crée-t-il un nouveau snapshot plutôt que de remplacer la table ?**
- Les fichiers sont **immuables** : on n'écrit jamais dans un fichier existant. Un changement = de nouveaux fichiers + une nouvelle version des métadonnées.
- Le commit se réduit à basculer un pointeur de `v1` vers `v2` : c'est atomique. Un lecteur voit l'ancienne version complète ou la nouvelle, jamais un mélange. Si le job plante avant la bascule, la table est intacte.
- Les anciennes versions restent lisibles (time travel, audit, rollback), et un lecteur qui a démarré sur `v1` peut finir sa requête même après le commit de `v2`.
- C'est aussi moins cher : un append de 3 lignes écrit un fichier de données d'environ 2 Ko et quelques Ko de métadonnées, sans réécrire les 84 Ko existants.

## Time travel

- Snapshots, du plus ancien au plus récent : `[6232720141900360718, 5025655001167750826]`.
- `VERSION AS OF 6232720141900360718` : **2 829** lignes. Table courante : **2 832**.
- La table `files` interrogée au premier snapshot liste **1** fichier, contre **2** pour la version courante.

**10. Que lit le time travel ?** L'état de la table tel que l'a enregistré ce snapshot : sa manifest list, ses manifests, donc exactement les fichiers de données qui le composaient. Le schéma utilisé est celui de ce snapshot (`latest_schema_id` dans `metadata_log_entries`).

**11. Pourquoi lire une ancienne version sans en faire de copie complète ?** Parce qu'un snapshot n'est qu'une **liste de fichiers**, et que les snapshots partagent ces fichiers. Le fichier de 84 Ko appartient aux deux snapshots ; le second n'ajoute qu'un fichier de 3 lignes. Garder une version coûte quelques Ko de métadonnées, plus les fichiers qui lui sont propres.

**12. Que se passe-t-il si on supprime physiquement les métadonnées et les fichiers de l'ancien snapshot ?**
- Le time travel vers ce snapshot échoue : snapshot inconnu si le metadata.json ne le liste plus, `FileNotFoundException` si ses fichiers ont disparu alors que les métadonnées les référencent encore.
- Danger particulier : les fichiers sont **partagés**. Ici, le fichier de données du premier snapshot fait aussi partie de la version courante. Le supprimer « à la main » casse la table courante, pas seulement l'historique.
- La bonne méthode passe par Iceberg : `CALL lab.system.expire_snapshots(...)` retire les snapshots des métadonnées et ne supprime que les fichiers qui ne sont plus référencés par aucun snapshot restant. `remove_orphan_files` nettoie ensuite les fichiers que plus rien ne référence (jobs plantés).

## Évolution de schéma

`ALTER TABLE lab.silver.orders ADD COLUMN source STRING`, puis `DESCRIBE` : `order_id string, user_id string, ordered_at timestamp, quantity int, product string, source string`.

| Entrée de `metadata_log_entries` | Snapshot courant | `latest_schema_id` | Événement |
|---|---|---|---|
| v1 (12:58:54) | 6232720141900360718 | 0 | Création |
| v2 (12:59:00) | 5025655001167750826 | 0 | Append de 3 lignes |
| v3 (12:59:03) | 5025655001167750826 | 0 | `ADD COLUMN` : nouvelle version de métadonnées, **même snapshot** |
| v4 (12:59:05) | 3965759359499948736 | 1 | `INSERT` : premier snapshot écrit avec le nouveau schéma |

Dans `v4.metadata.json` : `current-schema-id = 1`, avec `schema 0` (5 colonnes) et `schema 1` (6 colonnes, `source` en plus). Après l'`INSERT`, `source` vaut NULL pour 2 832 lignes et `'spark'` pour 1 ligne.

**13. Les fichiers Parquet existants ont-ils été réécrits ?** Non : `data files rewritten by ALTER TABLE: 0 | new data files: 0`. L'`ALTER TABLE` ne crée même pas de snapshot. À la lecture, une colonne absente d'un ancien fichier est renvoyée comme NULL. Iceberg identifie les colonnes par un **identifiant de champ** (field id), pas par leur nom ni leur position : c'est ce qui permet aussi de renommer ou de réordonner des colonnes sans toucher aux fichiers.

**14. Où le nouveau schéma est-il enregistré ?** Dans le fichier de métadonnées de la table : `v3.metadata.json` puis `v4.metadata.json` contiennent la liste `schemas` (0 et 1) et `current-schema-id = 1`. Chaque snapshot mémorise aussi le schéma avec lequel il a été écrit.

**15. Pourquoi l'évolution de schéma est-elle une opération de table et pas une modification d'un fichier Parquet ?**
- Une table, ce sont des dizaines ou des milliers de fichiers. Modifier chacun serait lent, coûteux, et non atomique : un lecteur verrait un mélange d'anciens et de nouveaux schémas.
- Le schéma de référence doit être unique et versionné pour tous les lecteurs et tous les moteurs. Le schéma de chaque fichier ne décrit que ce fichier, au moment où il a été écrit.
- Le time travel a besoin de connaître le schéma de chaque version.
- Avec Iceberg, le changement est une seule écriture de métadonnées, atomique comme n'importe quel commit. Les anciens fichiers restent valides et sont lus via les identifiants de colonne.

## Parquet vs Iceberg

| | `silver/spark/orders.parquet/` | `warehouse/silver/orders/` |
|---|---|---|
| Contenu | 1 part-file (139 556 o) + `_SUCCESS` | 3 fichiers de données, 3 manifests, 3 manifest lists, 4 `vN.metadata.json`, `version-hint.text` |
| Définition de « la table » | Tout ce que contient le dossier au moment du listing | Les fichiers du snapshot courant |
| Snapshots / historique | Aucun | 3 snapshots, 4 versions de métadonnées |
| Schéma | Dans le footer de chaque fichier | Versionné dans la table (2 schémas) |

**16. Lequel n'est qu'un ensemble de fichiers Parquet ?** `silver/spark/orders.parquet/`.

**17. Lequel a des métadonnées et des snapshots ?** `warehouse/silver/orders/` (table Iceberg).

**18. Comment remplacer sans risque un dossier Parquet pendant qu'une autre requête le lit ?**
- Ne jamais réécrire le dossier en place. Écrire la nouvelle version dans un **nouveau dossier** (par exemple `orders/version=2026-10-12T13/`), vérifier qu'il est complet (`_SUCCESS`, nombre de lignes).
- Basculer ensuite les lecteurs en une seule opération : changer la `LOCATION` d'une table Hive Metastore, une vue, ou un fichier pointeur lu par les consommateurs.
- Ne supprimer l'ancien dossier qu'une fois les lectures en cours terminées (délai de rétention).
- C'est exactement ce que fait Iceberg, en standard : nouveaux fichiers, puis bascule atomique d'un pointeur, puis expiration des vieilles versions.

**19. Comment Iceberg rend-il visible un nouvel état de façon atomique ?**
1. Écriture des nouveaux fichiers de données (invisibles : aucun manifest ne les cite).
2. Écriture des manifests et de la manifest list (le nouveau snapshot).
3. Écriture de `v(N+1).metadata.json`, qui pointe vers ce snapshot.
4. Bascule du pointeur du catalogue de `vN` vers `v(N+1)`. Cette seule opération est le commit.

Si deux écrivains commitent en même temps, le catalogue n'accepte que le premier (compare-and-swap). Le second relit la nouvelle version, réapplique ses changements et réessaie. Avec le catalogue Hadoop utilisé ici, le pointeur est un fichier sur S3 (`version-hint.text`) : sans rename atomique sur S3, cette protection n'est pas garantie en cas d'écritures concurrentes.

**20. Pourquoi faut-il un catalogue pour partager une table entre plusieurs moteurs ?**
- Tous les moteurs (Spark, Trino, Flink, DuckDB...) doivent s'accorder sur **quel `metadata.json` est le courant**. C'est le rôle du catalogue : associer un nom (`silver.orders`) à ce pointeur.
- Les commits doivent être sérialisés entre moteurs. Seul un service (catalogue REST/Polaris, Nessie, Glue, Hive Metastore, JDBC) offre un compare-and-swap fiable, ce que des fichiers sur S3 ne garantissent pas.
- Le catalogue apporte aussi la découverte des tables, les droits d'accès et un nommage commun. Sans lui, chaque moteur devrait connaître le chemin S3 et deviner la version courante.

## Exercices

**1. Créer `lab.silver.users` en Iceberg depuis `stg_users`.** 50 lignes, 1 snapshot `overwrite`.

**2. Ajouter un lot d'utilisateurs et inspecter les snapshots.** Lot construit à partir des 5 premiers `stg_users` par ordre alphabétique de `username`, avec un nouveau `user_id` (`uuid()`) et le suffixe `_b2` sur le `username`. Résultat : 2 snapshots (`overwrite` 50, puis `append` +5, 55 au total, 2 fichiers de données).

**3. Time travel avant / après.** `VERSION AS OF 8818426435690425568` : 50 lignes ; `VERSION AS OF 8989153037106919270` : 55 lignes. La différence (`EXCEPT` entre les deux versions) donne bien les 5 nouveaux utilisateurs : `ahancock_b2`, `atkinsbarbara_b2`, `blairamanda_b2`, `bruce43_b2`, `calebrussell_b2`.

**4. Ajouter `country` et écrire avec le nouveau schéma.** `ALTER TABLE lab.silver.users ADD COLUMN country STRING`, puis append de 3 utilisateurs avec `country = 'US'` (toutes les adresses du jeu sont américaines : état + zip). Résultat : 58 lignes, `country` NULL pour 55 et `US` pour 3 ; `current-schema-id = 1`, schéma 1 = schéma 0 + `country`. Les colonnes sont sélectionnées dans l'ordre de la table (`select(*cols)`) pour que l'append corresponde au nouveau schéma.

**5. Comparer les layouts S3 de users et orders.** Même structure : 3 fichiers de données, 3 manifests, 3 manifest lists, 4 `vN.metadata.json`, 1 `version-hint.text`. Les deux tables ont vécu la même histoire (création, append, évolution de schéma suivie d'une écriture), d'où le même nombre de fichiers. Seuls les volumes diffèrent : 66 187 o pour users, 136 882 o pour orders. Les métadonnées ne dépendent pas du contenu mais du nombre de commits et de fichiers.

**6. Parquet, Iceberg et catalogue Iceberg : la différence.**

| Niveau | Ce que c'est | Ce qu'il répond | Dans ce lab |
|---|---|---|---|
| Parquet | Un **format de fichier** colonnaire, compressé, avec schéma et statistiques dans le footer | « Que contient ce fichier ? » | `data/*.parquet`, `silver/spark/orders.parquet/` |
| Iceberg | Un **format de table** : une spécification de métadonnées (metadata.json, manifest lists, manifests) au-dessus de fichiers Parquet | « Quels fichiers forment la table, en quelle version, avec quel schéma ? » | `warehouse/silver/*/metadata/` |
| Catalogue Iceberg | Un **service de nommage et de commit** qui associe un nom de table à son metadata.json courant et garantit des commits atomiques | « Où est la table `silver.orders` et quelle est sa version courante ? » | catalogue `lab` de type `hadoop` (fichier `version-hint.text` sur S3) ; en production : REST, Nessie, Glue, Hive Metastore |

En une phrase : Parquet stocke les lignes, Iceberg définit la table, le catalogue dit quelle version de la table est la bonne.
