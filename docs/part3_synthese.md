# Partie 3 — Synthèse comparative RDD / DataFrame

> Source des chiffres : `notebooks/03_dataframe.ipynb`, exécution `app-20261114181835-0005`
> (fichiers `docs/logs/10_part3_benchmark.json`, `10_part3_results.json`, `10_part3_plans.txt`).
> **📏 Mesuré** = produit par le notebook. **📘 Théorie** = explication générale.
> Données : reconstitution de `producer.py` (fichier du professeur non fourni).

## 1. Équivalence des résultats (📏)

| Contrôle | Résultat |
|---|---|
| Lignes lues / non conformes / valeurs nulles | 150 281 / 0 / 0 |
| DELIVERED bruts → uniques (doublons écartés) | 46 977 → 46 028 (949) — identique à la Partie 2 |
| CA total | 8 799 645,53 MAD — identique à la Partie 2 |
| CA et panier moyen par ville : DataFrame = SQL = RDD | oui (écart < 0,01 MAD) |
| Top 5 restaurants : DataFrame = RDD | oui (mêmes identifiants, comptes et CA) |
| Jointure : commandes conservées | 46 028 / 46 028 (aucune perte ni duplication) |
| Chaque exécution du benchmark (chauffe + 5 passes × 6 variantes) | CA identique à la Partie 2 |

## 2. Protocole de mesure

- Une seule SparkSession ; 2 workers, **4 executors × 2 cœurs** (identiques avant/après, vérifié) ;
  **8 partitions de lecture** pour le RDD (`minPartitions=8`) comme pour le DataFrame (vérifié) ;
  `spark.sql.shuffle.partitions = 8`.
- Même logique métier : `DELIVERED` → dédoublonnage par `order_id` → somme de `amount_mad` par ville.
- 1 exécution de chauffe par variante, puis **5 passes entrelacées avec rotation de l'ordre**.
- Tâches, stages, temps executor et octets relevés par l'API REST de Spark, ramenés à une exécution.
- **Scénario A** : de bout en bout depuis S3, sans cache (vérifié : aucun `InMemoryTableScan` dans les plans).
- **Scénario B** : sources déjà en mémoire, pour isoler le coût du calcul de celui du parsing JSON.

## 3. Résultats du benchmark (📏)

### Scénario A — lecture S3 + calcul (sans cache)

| Variante | Médiane | Moyenne | Écart-type | Jobs | Stages exécutés | Tâches | Temps executors | Lu | Shuffle écrit | Accélération |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| RDD (PySpark) | 0,847 s | 0,848 s | 0,151 s | 1 | 3 | 24 | 5,04 s | 34,4 Mo | 580 Ko | 1,00× |
| DataFrame (AQE) | 0,369 s | 0,347 s | 0,041 s | 3 | 3 | 10 | 0,93 s | 34,4 Mo | 838 Ko | **2,30×** |
| DataFrame sans AQE | 0,335 s | 0,337 s | 0,046 s | 1 | 3 | 24 | 1,18 s | 34,4 Mo | 841 Ko | **2,53×** |
| SQL | 0,357 s | 0,373 s | 0,046 s | 3 | 3 | 10 | 0,99 s | 34,4 Mo | 838 Ko | **2,37×** |

### Scénario B — données déjà en cache

| Variante | Médiane | Moyenne | Écart-type | Tâches | Temps executors | Accélération |
|---|---:|---:|---:|---:|---:|---:|
| RDD (cache) | 0,637 s | 0,619 s | 0,077 s | 24 | 3,76 s | 1,00× |
| DataFrame (cache) | 0,224 s | 0,234 s | 0,051 s | 10 | 0,29 s | **2,84×** |

**Reproductibilité** : une exécution valide précédente (`app-…-0004`) donnait 2,11× à 2,51× (A) et 2,16× (B).
L'ordre de grandeur (**DataFrame environ 2 à 3 fois plus rapide**) est stable ; la valeur exacte varie de ±20 %.

### Autres mesures

| Mesure | Résultat |
|---|---|
| Lecture avec schéma explicite vs inférence | 0,03–0,06 s et **0 job** vs 0,5–1,6 s et **1 job** (lecture complète) |
| Types inférés | `items_count` → `bigint`, `event_time` → `string` (perte du type `timestamp`) |
| Jointure choisie par Catalyst | **BroadcastHashJoin** (référentiel < 10 Mo) |
| BroadcastHashJoin vs SortMergeJoin forcé | médianes 0,450 s vs 0,492 s (écart faible à ce volume) |
| `distance_band` : UDF Python / UDF pandas / `when` | 0,346 s / 0,281 s / **0,134 s** (natif 2,6× plus rapide que l'UDF Python) |
| Opérateurs dans les plans | `BatchEvalPython` (UDF), `ArrowEvalPython` (pandas UDF), aucun pour `when` |
| Silver Parquet | 147 244 lignes, 8 partitions `city=…`, **4,3 Mio** (contre 32 Mio de JSON brut) |
| Élagage de partitions | `PartitionFilters: [city = Casablanca]` : seul le dossier concerné est lu |

## 4. Interprétation

**Mesuré** — d'où vient l'écart ?
- Le DataFrame consomme **5,4× moins de temps executor** (0,93 s contre 5,04 s) pour le même volume lu
  (34,4 Mo). Le gain ne vient donc pas d'une lecture plus légère mais du **coût de traitement par ligne**.
- Le gain persiste avec les données en cache (scénario B : 2,84×), donc il ne se limite pas au parsing
  JSON (`json.loads` en Python contre parseur Jackson dans la JVM) : les lambdas Python du RDD
  (`filter`, `map`, `reduceByKey`) coûtent aussi.
- **Le DataFrame écrit plus d'octets de shuffle** (838 Ko contre 580 Ko). Le dédoublonnage
  `dropDuplicates` est exécuté en `SortAggregate` avec des tampons `first`/`valueSet`, alors que le RDD
  envoie des paires Python sérialisées puis compressées par lots. Tungsten n'a donc **pas** réduit le
  volume de shuffle ici : son gain porte sur le CPU, pas sur le réseau.
- **AQE n'accélère pas** ce petit volume : il réduit les tâches (24 → 10, partitions fusionnées) mais
  découpe la requête en 3 jobs. La variante sans AQE est légèrement plus rapide (0,335 s contre 0,369 s).
  Les « stages ignorés » des variantes AQE sont un artefact de ce découpage, pas du travail en moins.
- DataFrame et SQL ont des **métriques d'exécution identiques** (3 jobs, 10 tâches, 838 Ko de shuffle)
  et des temps équivalents (écart dans le bruit) : les deux API passent par le même optimiseur.

**📘 Théorie** — pourquoi le DataFrame est plus efficace :
- **Catalyst** voit la requête entière : il pousse le filtre dans le lecteur
  (`PushedFilters: [IsNotNull(status), EqualTo(status,DELIVERED)]`, 📏) et ne matérialise que les
  colonnes utiles (`ReadSchema: order_id, city, status, amount_mad`, 📏).
  Pour du JSON (format ligne), le fichier est tout de même lu en entier : le filtre poussé évite la
  **conversion** des lignes rejetées, pas leur lecture.
- **Tungsten** génère du bytecode JVM pour des blocs d'opérateurs (4 blocs `codegen id` dans le plan, 📏)
  et manipule des lignes binaires (`UnsafeRow`) au lieu d'objets Python.
- Le **RDD PySpark** exécute chaque lambda dans un processus Python séparé : chaque élément est sérialisé
  (pickle) de la JVM vers Python puis renvoyé. Le lineage est une chaîne de fonctions **opaques** que Spark
  ne peut ni réordonner ni simplifier.

## 5. Lineage RDD vs plan DataFrame

| | Lineage RDD (`toDebugString`) | Plan DataFrame (`explain`) |
|---|---|---|
| Nature | **comment** calculer : suite de RDD et de fonctions | **quoi** calculer : plan relationnel |
| Optimisable | non (fonctions opaques) | oui : 4 niveaux (analysé, optimisé, physique, code généré) |
| Frontières de stage | indentation `+-` aux `reduceByKey` | nœuds `Exchange` (hashpartitioning) |
| Pré-agrégation | combiner de `reduceByKey` | `HashAggregate partial` → `Exchange` → `HashAggregate final` |
| Tolérance aux pannes | recalcul via le lineage | identique : le plan physique est exécuté en RDD internes |

## 6. Limites de la comparaison

1. **Biais PySpark** : l'écart mêle l'optimisation Catalyst/Tungsten et le coût du passage par Python.
   En Scala, les RDD restent dans la JVM et l'écart serait plus faible. Ce n'est pas mesuré ici.
2. **Petit volume** (32 Mio, environ 0,3 à 0,8 s par requête) : les coûts fixes (planification, lancement des
   tâches, AQE) pèsent lourd. À plus grande échelle, les proportions peuvent changer.
3. **Variabilité entre sessions** : le même pipeline RDD a mesuré 1,58 s (médiane) en Partie 2 et 0,85 s ici.
   Seules les mesures **d'une même session**, entrelacées, sont comparables.
4. **Piège évité** : le cache d'un DataFrame est réutilisé automatiquement par toute requête de même plan
   logique. Une première série de mesures en a été faussée (DataFrame lu en mémoire). Elle a été écartée et
   le protocole vérifie désormais les plans.
5. **Silver** : 8 petits fichiers (environ 64–150 Kio) par ville. Un `repartition("city")` avant l'écriture
   en produirait un seul par partition (amélioration possible).
6. **Dataset** : non disponible en PySpark, donc comparé uniquement sur le plan théorique.
