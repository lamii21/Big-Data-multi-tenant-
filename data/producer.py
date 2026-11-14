#!/usr/bin/env python3
"""
=============================================================================
 RECONSTITUTION — ce fichier N'EST PAS le producer.py fourni par le professeur
 (absent des fichiers du sujet). Il est reconstitué d'après le cahier des
 charges (§2.1, §3.2.1, §3.4.1) et documente explicitement ses hypothèses.
=============================================================================

Générateur d'événements de commandes de livraison (marché marocain, MAD).
Un événement JSON est émis à chaque changement d'état d'une commande :

    CREATED ──► PICKED_UP ──► DELIVERED
       │            │
       └──► CANCELLED ◄┘          (annulation avant ou après la prise en charge)

Schéma d'un événement (ordre des champs conforme au sujet) :
    order_id       str    "ORD-0000123"      identifiant de commande (7 chiffres)
    restaurant_id  str    "R-045"            référence restaurants.json
    customer_id    str    "C-08812"          client (même ville que le restaurant)
    city           str    "Casablanca"       ville du restaurant
    status         str    CREATED | PICKED_UP | DELIVERED | CANCELLED
    amount_mad     float  145.50             montant de la commande en dirhams
    items_count    int    3                  nombre d'articles
    distance_km    float  4.2                distance restaurant -> client
    courier_id     str    "L-CAS-07"         livreur (code ville sur 3 lettres)
    event_time     str    "2026-11-15T12:34:56Z"  horodatage UTC de l'événement

Défauts injectés volontairement (cas réels, cf. sujet) :
  * ~5 % d'événements EN RETARD : event_time conserve l'instant réel du
    changement d'état, mais le message est émis jusqu'à 15 min plus tard
    (livreur hors réseau). => test du watermark (Partie 4).
  * ~2 % de DOUBLONS : le même message (même order_id, même status, même
    contenu) est ré-émis 1 à 30 s plus tard (retransmission).
    => test de dropDuplicates (Partie 5).

-----------------------------------------------------------------------------
MODES
-----------------------------------------------------------------------------
  --mode file   Écrit un historique JSON Lines (Parties 2 et 3).
                --n = nombre de COMMANDES (chaque commande produit 2 à 3
                événements). Les lignes sont triées par ordre d'ARRIVÉE :
                un événement en retard apparaît donc après des événements
                plus récents, comme dans Kafka.
                Entièrement déterministe pour une graine donnée.

  --mode kafka  Publie en continu dans le topic Kafka (Parties 4 et 5).
                --rate = événements par seconde (tous statuts confondus).
                Clé du message = order_id : tous les événements d'une commande
                vont dans la même partition (ordre préservé par commande).
                event_time = horloge réelle (UTC). Les order_id démarrent à
                1 000 000 (pas de collision avec l'historique) et le compteur
                est persisté entre deux lancements (fichier d'état).

-----------------------------------------------------------------------------
EXEMPLES
-----------------------------------------------------------------------------
  # Historique de 50 000 commandes (Partie 2)
  python3 data/producer.py --mode file --n 50000

  # Flux temps réel à 20 événements/s (Partie 4) — Ctrl+C pour arrêter
  python3 data/producer.py --mode kafka --rate 20

  # Démo accélérée : cycle de vie 10x plus rapide, arrêt après 5 minutes
  python3 data/producer.py --mode kafka --rate 20 --time-scale 10 --duration 300

Lancé depuis l'hôte : docker compose exec jupyter python3 data/producer.py ...
(variable KAFKA_BOOTSTRAP=kafka:9092 déjà définie dans le conteneur ;
 depuis Windows, utiliser --bootstrap localhost:9094).
"""
from __future__ import annotations

import argparse
import heapq
import json
import os
import random
import signal
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
STATUSES = ("CREATED", "PICKED_UP", "DELIVERED", "CANCELLED")

# Prix unitaire moyen d'un article par catégorie (MAD, min/max)
UNIT_PRICE_MAD = {
    "marocain": (45, 110), "pizza": (40, 110), "tacos": (30, 65), "burger": (35, 85),
    "sushi": (60, 140), "shawarma": (25, 55), "healthy": (45, 95), "patisserie": (15, 45),
}
DEFAULT_UNIT_PRICE = (30, 90)
ITEMS_WEIGHTS = {1: 25, 2: 30, 3: 20, 4: 12, 5: 8, 6: 5}

# Profil horaire de la demande en heure LOCALE marocaine (pics déjeuner/dîner)
HOUR_WEIGHTS_LOCAL = [1.0, 0.5, 0.3, 0.2, 0.2, 0.3, 0.6, 1.2, 2.0, 2.5, 3.0, 5.0,
                      9.0, 10.0, 7.0, 4.0, 3.5, 4.0, 6.0, 9.0, 10.0, 8.0, 5.0, 2.5]
MOROCCO_UTC_OFFSET_H = 1  # Maroc = UTC+1 (hors période de Ramadan, simplifié)

N_CUSTOMERS = 20_000
KAFKA_FIRST_ORDER_NUM = 1_000_000
STATE_FILE = HERE / "generated" / ".producer_state.json"


# =============================================================================
# Modèle : restaurants, clients, livreurs
# =============================================================================
@dataclass(frozen=True)
class Order:
    order_id: str
    restaurant_id: str
    customer_id: str
    city: str
    amount_mad: float
    items_count: int
    distance_km: float
    courier_id: str


def load_restaurants(path: Path) -> list[dict]:
    text = path.read_text(encoding="utf-8").strip()
    rows = json.loads(text) if text.startswith("[") else [json.loads(l) for l in text.splitlines() if l.strip()]
    if not rows:
        sys.exit(f"Aucun restaurant dans {path}")
    return rows


def city_code(city: str) -> str:
    """Casablanca -> CAS (format du sujet : L-CAS-07)."""
    return city[:3].upper()


class World:
    """Population simulée, construite de façon déterministe depuis la graine."""

    def __init__(self, restaurants: list[dict], rng: random.Random, cancel_ratio: float):
        self.rng = rng
        self.cancel_ratio = cancel_ratio
        self.by_city: dict[str, list[dict]] = {}
        for r in restaurants:
            self.by_city.setdefault(r["city"], []).append(r)
        self.cities = sorted(self.by_city)
        # Popularité des restaurants (log-normale) => un vrai "top 5" émerge
        self.rest_weights = {c: [rng.lognormvariate(0, 0.7) for _ in rs] for c, rs in self.by_city.items()}
        # Poids des villes = somme des popularités de leurs restaurants
        self.city_weights = [sum(self.rest_weights[c]) for c in self.cities]

        # Clients : blocs contigus C-00001..C-20000 répartis au prorata des restaurants
        total = len(restaurants)
        self.customers: dict[str, tuple[int, int]] = {}
        start = 1
        for i, c in enumerate(self.cities):
            n = N_CUSTOMERS - (start - 1) if i == len(self.cities) - 1 else round(N_CUSTOMERS * len(self.by_city[c]) / total)
            self.customers[c] = (start, start + n - 1)
            start += n

        # Livreurs : un effectif par ville (>= 8), avec des cycles travail/pause
        # en minutes simulées. Pause (60-120 min) > durée max d'une livraison :
        # les sessions d'un livreur sont séparées par > 5 min d'inactivité
        # (fenêtre session_window de la Partie 5).
        self.couriers: dict[str, list[tuple[str, float, float, float]]] = {}
        for c in self.cities:
            n = min(99, max(8, len(self.by_city[c])))
            self.couriers[c] = []
            for k in range(1, n + 1):
                work, pause = rng.uniform(30, 90), rng.uniform(60, 120)
                self.couriers[c].append((f"L-{city_code(c)}-{k:02d}", work, pause, rng.uniform(0, work + pause)))

    def pick_courier(self, city: str, sim_minutes: float) -> str:
        active = [cid for cid, work, pause, phase in self.couriers[city]
                  if (sim_minutes + phase) % (work + pause) < work]
        pool = active or [c[0] for c in self.couriers[city]]
        return self.rng.choice(pool)

    def new_order(self, order_num: int, sim_minutes: float) -> tuple[Order, list[tuple[float, str]]]:
        rng = self.rng
        city = rng.choices(self.cities, weights=self.city_weights)[0]
        rest = rng.choices(self.by_city[city], weights=self.rest_weights[city])[0]
        lo, hi = self.customers[city]
        items = rng.choices(list(ITEMS_WEIGHTS), weights=list(ITEMS_WEIGHTS.values()))[0]
        pmin, pmax = UNIT_PRICE_MAD.get(rest["category"], DEFAULT_UNIT_PRICE)
        distance = round(min(15.0, max(0.3, rng.gammavariate(2.0, 1.9))), 1)
        delivery_fee = min(30.0, 9.0 + 1.5 * distance)
        amount = round(sum(rng.uniform(pmin, pmax) for _ in range(items)) + delivery_fee, 2)
        order = Order(
            order_id=f"ORD-{order_num:07d}",
            restaurant_id=rest["restaurant_id"],
            customer_id=f"C-{rng.randint(lo, hi):05d}",
            city=city,
            amount_mad=amount,
            items_count=items,
            distance_km=distance,
            courier_id=self.pick_courier(city, sim_minutes),
        )
        return order, self.lifecycle(distance)

    def lifecycle(self, distance: float) -> list[tuple[float, str]]:
        """Liste (minutes après création, statut). Transitions toujours valides."""
        rng = self.rng
        steps = [(0.0, "CREATED")]
        r = rng.random()
        if r < self.cancel_ratio * 0.7:                       # annulée avant prise en charge
            steps.append((rng.uniform(2, 15), "CANCELLED"))
            return steps
        prep = rng.uniform(8, 25)                             # préparation au restaurant
        steps.append((prep, "PICKED_UP"))
        if r < self.cancel_ratio:                             # annulée pendant la livraison
            steps.append((prep + rng.uniform(5, 20), "CANCELLED"))
        else:
            steps.append((prep + 4 + distance * 3.5 * rng.uniform(0.8, 1.3), "DELIVERED"))
        return steps


def build_event(order: Order, status: str, epoch_s: float) -> dict:
    return {
        "order_id": order.order_id,
        "restaurant_id": order.restaurant_id,
        "customer_id": order.customer_id,
        "city": order.city,
        "status": status,
        "amount_mad": order.amount_mad,
        "items_count": order.items_count,
        "distance_km": order.distance_km,
        "courier_id": order.courier_id,
        "event_time": datetime.fromtimestamp(int(epoch_s), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def dumps(event: dict) -> str:
    return json.dumps(event, ensure_ascii=False, separators=(",", ":"))


# =============================================================================
# Mode FILE
# =============================================================================
def run_file(args, world: World, rng: random.Random) -> None:
    start = datetime.fromisoformat(args.start.replace("Z", "+00:00")).astimezone(timezone.utc)
    start_s = start.timestamp()

    # 1) Instants de création : jour uniforme, heure locale selon le profil de demande
    creations = []
    for _ in range(args.n):
        day = rng.randrange(args.days)
        hour_local = rng.choices(range(24), weights=HOUR_WEIGHTS_LOCAL)[0]
        sec = rng.randrange(3600)
        creations.append(start_s + day * 86400 + (hour_local - MOROCCO_UTC_OFFSET_H) * 3600 + sec)
    creations.sort()  # order_id croissants dans le temps

    # 2) Cycle de vie + injection des retards et doublons ; clé de tri = arrivée
    records: list[tuple[float, int, str]] = []
    seq = 0
    stats = Counter()
    for i, t0 in enumerate(creations, start=1):
        order, steps = world.new_order(i, (t0 - start_s) / 60)
        for offset_min, status in steps:
            t_event = int(t0 + offset_min * 60)
            line = dumps(build_event(order, status, t_event))
            stats[status] += 1
            arrival = float(t_event)
            if rng.random() < args.late_ratio:
                delay = rng.uniform(1, args.max_late_minutes) * 60
                arrival += delay
                stats["late"] += 1
                if delay > 600:
                    stats["late_over_10min"] += 1
            records.append((arrival, seq, line)); seq += 1
            if rng.random() < args.dup_ratio:
                records.append((arrival + rng.uniform(1, 30), seq, line)); seq += 1
                stats["duplicates"] += 1
    records.sort()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="\n") as fh:
        for _, _, line in records:
            fh.write(line + "\n")

    originals = sum(stats[s] for s in STATUSES)
    meta = {
        "generator": "producer.py (RECONSTITUTION)", "mode": "file", "seed": args.seed,
        "orders": args.n, "events_original": originals, "lines_written": len(records),
        "late_injected": stats["late"], "late_over_10min": stats["late_over_10min"],
        "duplicates_injected": stats["duplicates"],
        "status_counts": {s: stats[s] for s in STATUSES},
        "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"), "days": args.days,
        "late_ratio": args.late_ratio, "dup_ratio": args.dup_ratio, "cancel_ratio": args.cancel_ratio,
    }
    args.output.with_suffix(".stats.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"{args.n} commandes -> {len(records)} lignes écrites dans {args.output}")
    print(json.dumps(meta, indent=2))


# =============================================================================
# Mode KAFKA
# =============================================================================
def load_next_order_num(explicit: int | None) -> int:
    if explicit is not None:
        return explicit
    try:
        return int(json.loads(STATE_FILE.read_text())["next_order_num"])
    except (FileNotFoundError, KeyError, ValueError):
        return KAFKA_FIRST_ORDER_NUM


def save_next_order_num(n: int) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps({"next_order_num": n}), encoding="utf-8")


def run_kafka(args, world: World, rng: random.Random) -> None:
    try:
        from kafka import KafkaProducer  # kafka-python
    except ImportError:
        sys.exit("Le mode kafka nécessite kafka-python (pip install kafka-python).")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        acks="all",
        linger_ms=10,
        retries=5,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: dumps(v).encode("utf-8"),
    )
    # docker stop envoie SIGTERM : on le traite comme Ctrl+C (flush + état sauvegardé)
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))

    next_num = load_next_order_num(args.start_id)
    first_num = next_num
    heap: list[tuple[float, int, str, object]] = []  # (échéance murale, seq, type, charge)
    seq = 0
    stats = Counter()
    t_begin = time.time()
    second = 0

    def push(due: float, kind: str, payload) -> None:
        nonlocal seq
        heapq.heappush(heap, (due, seq, kind, payload)); seq += 1

    def route(event: dict, t_event: float, batch: list) -> None:
        """Émission immédiate, ou différée (retard) ; doublon éventuel."""
        arrival = t_event
        if rng.random() < args.late_ratio:
            arrival = t_event + rng.uniform(1, args.max_late_minutes) * 60
            push(arrival, "send", event)
            stats["late_scheduled"] += 1
        else:
            batch.append(event)
        if rng.random() < args.dup_ratio:
            push(arrival + rng.uniform(1, 30), "send", event)
            stats["dup_scheduled"] += 1

    print(f"[producer] Kafka {args.bootstrap} topic={args.topic} rate={args.rate}/s "
          f"time-scale={args.time_scale} premier order_id=ORD-{first_num:07d}", flush=True)
    try:
        while True:
            if args.duration and second >= args.duration:
                break
            tick = t_begin + second
            pause = tick - time.time()
            if pause > 0:
                time.sleep(pause)
            now = time.time()
            batch: list[dict] = []

            # 1) Événements arrivés à échéance (transitions, retards, doublons)
            while heap and heap[0][0] <= now:
                due, _, kind, payload = heapq.heappop(heap)
                if kind == "transition":
                    order, status = payload
                    route(build_event(order, status, due), due, batch)
                else:
                    batch.append(payload)
                    stats["late_or_dup_sent"] += 1

            # 2) Nouvelles commandes pour compléter le débit cible
            k = max(0, args.rate - len(batch))
            if args.n:
                k = min(k, args.n - (next_num - first_num))
            for i in range(k):
                t0 = now + i / max(k, 1)
                sim_min = (t0 - t_begin) * args.time_scale / 60
                order, steps = world.new_order(next_num, sim_min)
                next_num += 1
                for offset_min, status in steps:
                    if offset_min == 0:
                        route(build_event(order, status, t0), t0, batch)
                    else:
                        push(t0 + offset_min * 60 / args.time_scale, "transition", (order, status))

            # 3) Envoi (clé = order_id)
            for ev in batch:
                producer.send(args.topic, key=ev["order_id"], value=ev)
                stats[ev["status"]] += 1
                stats["sent"] += 1
            producer.flush()

            second += 1
            if second % 10 == 0:
                el = time.time() - t_begin
                print(f"[{el:6.0f}s] envoyés={stats['sent']} ({stats['sent'] / el:.1f}/s) "
                      f"commandes={next_num - first_num} en_attente={len(heap)} "
                      f"retards_prog={stats['late_scheduled']} doublons_prog={stats['dup_scheduled']} "
                      + " ".join(f"{s}={stats[s]}" for s in STATUSES), flush=True)
            if args.n and next_num - first_num >= args.n and not heap:
                break
    except KeyboardInterrupt:
        print("\n[producer] arrêt demandé", flush=True)
    finally:
        producer.flush()
        producer.close()
        save_next_order_num(next_num)
        summary = {
            "orders_created": next_num - first_num,
            "first_order_id": f"ORD-{first_num:07d}", "last_order_id": f"ORD-{next_num - 1:07d}",
            "messages_sent": stats["sent"], "pending_not_sent": len(heap),
            "late_scheduled": stats["late_scheduled"], "dup_scheduled": stats["dup_scheduled"],
            "status_counts": {s: stats[s] for s in STATUSES},
            "elapsed_s": round(time.time() - t_begin, 1),
        }
        print("[producer] résumé " + json.dumps(summary), flush=True)


# =============================================================================
# CLI
# =============================================================================
def positive_ratio(value: str) -> float:
    v = float(value)
    if not 0 <= v <= 1:
        raise argparse.ArgumentTypeError("doit être compris entre 0 et 1")
    return v


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        prog="producer.py",
        description="RECONSTITUTION du générateur de commandes (fichier du professeur non fourni).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--mode", choices=["file", "kafka"], required=True,
                   help="file = historique JSON Lines ; kafka = flux continu vers le topic")
    p.add_argument("--n", type=int, default=None,
                   help="nombre de COMMANDES (file : défaut 50000 ; kafka : illimité)")
    p.add_argument("--seed", type=int, default=42, help="graine aléatoire (reproductibilité)")
    p.add_argument("--restaurants", type=Path, default=HERE / "restaurants.json",
                   help="référentiel des restaurants (JSON Lines ou tableau JSON)")
    p.add_argument("--late-ratio", type=positive_ratio, default=0.05,
                   help="proportion d'événements émis en retard")
    p.add_argument("--max-late-minutes", type=float, default=15.0,
                   help="retard maximal d'un événement (minutes, retard minimal = 1)")
    p.add_argument("--dup-ratio", type=positive_ratio, default=0.02,
                   help="proportion d'événements ré-émis en doublon")
    p.add_argument("--cancel-ratio", type=positive_ratio, default=0.08,
                   help="proportion de commandes annulées")

    f = p.add_argument_group("mode file")
    f.add_argument("--output", type=Path, default=HERE / "generated" / "orders_history.jsonl",
                   help="fichier de sortie (un fichier .stats.json est écrit à côté)")
    f.add_argument("--start", default="2026-10-01T00:00:00Z", help="début de la période simulée (UTC)")
    f.add_argument("--days", type=int, default=30, help="durée de la période simulée (jours)")

    k = p.add_argument_group("mode kafka")
    k.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"),
                   help="serveurs Kafka (env KAFKA_BOOTSTRAP)")
    k.add_argument("--topic", default="orders", help="topic cible")
    k.add_argument("--rate", type=int, default=20, help="débit cible en événements par seconde")
    k.add_argument("--duration", type=int, default=0, help="durée en secondes (0 = jusqu'à Ctrl+C)")
    k.add_argument("--time-scale", type=float, default=1.0,
                   help="accélération du cycle de vie (10 = livraisons 10x plus rapides) ; "
                        "les retards réseau et doublons restent en temps réel")
    k.add_argument("--start-id", type=int, default=None,
                   help=f"premier numéro de commande (défaut : reprise du fichier d'état, sinon {KAFKA_FIRST_ORDER_NUM})")
    args = p.parse_args(argv)
    if args.mode == "file" and args.n is None:
        args.n = 50_000
    if args.n is not None and args.n <= 0:
        p.error("--n doit être > 0")
    if args.rate <= 0 or args.time_scale <= 0:
        p.error("--rate et --time-scale doivent être > 0")
    if args.max_late_minutes < 1:
        p.error("--max-late-minutes doit être >= 1")
    return args


def main(argv=None) -> None:
    args = parse_args(argv)
    rng = random.Random(args.seed)
    world = World(load_restaurants(args.restaurants), rng, args.cancel_ratio)
    if args.mode == "file":
        run_file(args, world, rng)
    else:
        run_kafka(args, world, rng)


if __name__ == "__main__":
    main()
