# `data/` — données du projet

> ⚠️ **Fichiers RECONSTITUÉS.** Le sujet mentionne un générateur `producer.py`
> et un référentiel `restaurants.json` « fournis », mais ces fichiers n'ont pas
> été transmis. Les fichiers de ce dossier sont des **reconstitutions** écrites
> par le binôme d'après le cahier des charges (§2.1, §3.2.1, §3.4.1). Ils ne
> doivent pas être présentés comme les fichiers originaux du professeur.
> Si les originaux sont fournis plus tard, ils remplaceront ces fichiers sans
> changement côté Spark tant que le schéma reste celui du §2.1.

| Fichier | Rôle | Versionné |
|---|---|---|
| `generate_restaurants.py` | Génère `restaurants.json` (déterministe, `--seed`) | oui |
| `restaurants.json` | Référentiel statique : 200 restaurants (JSON Lines) | oui |
| `producer.py` | Générateur d'événements `orders` : modes `file` et `kafka` | oui |
| `generated/orders_history.jsonl` | Historique produit par `--mode file` (Parties 2-3) | non (régénérable) |
| `generated/orders_history.stats.json` | Comptage des retards/doublons injectés | non |
| `generated/.producer_state.json` | Prochain `order_id` du mode kafka (reprise) | non |

## Référentiel `restaurants.json`

Une ligne JSON par restaurant (lisible par `spark.read.json` sans `multiLine`) :

```json
{"restaurant_id": "R-001", "name": "Burger Lab Bourgogne", "city": "Casablanca", "category": "burger", "latitude": 33.542277, "longitude": -7.556894}
```

- 8 villes : Casablanca 50, Rabat 30, Marrakech 30, Fes 22, Tanger 22, Agadir 18, Meknes 14, Oujda 14.
- 8 catégories : marocain, pizza, tacos, burger, sushi, shawarma, healthy, patisserie.
- Coordonnées tirées dans l'emprise urbaine terrestre de chaque ville.
- Noms de villes **en ASCII** (`Fes`, `Meknes`) : ils deviennent des noms de partitions (`city=Fes`).

## Événements `orders`

```json
{"order_id":"ORD-0000123","restaurant_id":"R-045","customer_id":"C-08812","city":"Casablanca","status":"DELIVERED","amount_mad":145.5,"items_count":3,"distance_km":4.2,"courier_id":"L-CAS-07","event_time":"2026-11-15T12:34:56Z"}
```

Cycle de vie : `CREATED → PICKED_UP → DELIVERED`, ou annulation (`CANCELLED`)
avant ou après la prise en charge. ~8 % de commandes annulées.

| Hypothèse de modélisation | Valeur |
|---|---|
| Préparation (CREATED → PICKED_UP) | 8 à 25 min |
| Livraison (PICKED_UP → DELIVERED) | 4 min + 3,5 min/km (±30 %) |
| `amount_mad` | articles × prix unitaire de la catégorie + frais de livraison (9 + 1,5 MAD/km, max 30) |
| `distance_km` | loi gamma, moyenne ≈ 3,8 km, bornée 0,3–15 km |
| Clients | 20 000 (`C-00001`…`C-20000`), rattachés à une ville |
| Livreurs | `L-<3 lettres de la ville>-NN`, cycles travail 30–90 min / pause 60–120 min |
| Demande | pics déjeuner et dîner en heure locale (UTC+1) |
| Retards | ~5 % des événements, émis 1 à 15 min après `event_time` |
| Doublons | ~2 % des événements, copie exacte ré-émise 1 à 30 s après |

## Commandes

```bash
# Référentiel (déjà versionné ; à relancer seulement pour le régénérer)
docker compose exec jupyter python3 data/generate_restaurants.py --seed 42

# Historique 50 000 commandes (Partie 2)
docker compose exec jupyter python3 data/producer.py --mode file --n 50000

# Flux Kafka 20 événements/s (Partie 4) — Ctrl+C pour arrêter
docker compose exec jupyter python3 data/producer.py --mode kafka --rate 20

# Aide complète des arguments
docker compose exec jupyter python3 data/producer.py --help

# Validation indépendante des données
docker compose exec jupyter python3 scripts/validate_generated_data.py \
    --restaurants data/restaurants.json \
    --events data/generated/orders_history.jsonl --expected-orders 50000 --complete
```
