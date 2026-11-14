#!/usr/bin/env python3
"""
Validation des données RECONSTITUÉES (restaurants.json + événements de producer.py).

Ce script est indépendant du générateur : il relit les fichiers produits et
vérifie le schéma, les formats, la cohérence référentielle, les transitions de
statut et les proportions de retards / doublons. Code de sortie 1 si un
contrôle bloquant échoue.

Usage :
  # Référentiel seul
  python3 scripts/validate_generated_data.py --restaurants data/restaurants.json

  # Historique (mode file) : commandes complètes attendues
  python3 scripts/validate_generated_data.py --restaurants data/restaurants.json \
      --events data/generated/orders_history.jsonl --expected-orders 50000 --complete

  # Messages consommés depuis Kafka (kafka-console-consumer avec
  # print.timestamp=true print.partition=true print.offset=true print.key=true)
  python3 scripts/validate_generated_data.py --restaurants data/restaurants.json \
      --events docs/logs/kafka_sample.txt --format kafka-console
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "data"))
from generate_restaurants import CATEGORIES, CITIES  # noqa: E402  (emprises géographiques de référence)

RESTAURANT_FIELDS = ["restaurant_id", "name", "city", "category", "latitude", "longitude"]
EVENT_FIELDS = ["order_id", "restaurant_id", "customer_id", "city", "status", "amount_mad",
                "items_count", "distance_km", "courier_id", "event_time"]
STATIC_FIELDS = ["restaurant_id", "customer_id", "city", "amount_mad", "items_count", "distance_km", "courier_id"]
STATUSES = {"CREATED", "PICKED_UP", "DELIVERED", "CANCELLED"}
# Séquences de statuts valides (ordonnées par event_time)
VALID_COMPLETE = {("CREATED", "PICKED_UP", "DELIVERED"), ("CREATED", "CANCELLED"),
                  ("CREATED", "PICKED_UP", "CANCELLED")}
RE = {
    "order_id": re.compile(r"^ORD-\d{7}$"),
    "restaurant_id": re.compile(r"^R-\d{3}$"),
    "customer_id": re.compile(r"^C-\d{5}$"),
    "courier_id": re.compile(r"^L-[A-Z]{3}-\d{2}$"),
    "event_time": re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"),
}
LATE_THRESHOLD_S = 45  # au-delà : événement considéré comme arrivé en retard


class Report:
    def __init__(self) -> None:
        self.checks: list[tuple[str, bool, str]] = []
        self.metrics: dict = {}

    def check(self, name: str, ok: bool, detail: str = "") -> None:
        self.checks.append((name, bool(ok), detail))
        print(f"  [{'OK' if ok else 'KO'}] {name}" + (f" — {detail}" if detail else ""))

    @property
    def ok(self) -> bool:
        return all(ok for _, ok, _ in self.checks)


def ts(s: str) -> float:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()


# =============================================================================
def validate_restaurants(path: Path, rep: Report) -> dict[str, dict]:
    print(f"\n=== Référentiel {path}")
    lines = [l for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]
    rows = [json.loads(l) for l in lines]
    rep.check("200 restaurants exactement", len(rows) == 200, f"{len(rows)} lignes")
    rep.check("champs exacts et ordonnés",
              all(list(r) == RESTAURANT_FIELDS for r in rows), ", ".join(RESTAURANT_FIELDS))
    ids = [r["restaurant_id"] for r in rows]
    rep.check("restaurant_id uniques", len(set(ids)) == len(ids))
    rep.check("restaurant_id séquentiels R-001..R-200",
              ids == [f"R-{i:03d}" for i in range(1, len(rows) + 1)])
    rep.check("noms uniques et non vides", len({r["name"] for r in rows}) == len(rows) and all(r["name"] for r in rows))
    rep.check("catégories connues", all(r["category"] in CATEGORIES for r in rows),
              ", ".join(sorted(Counter(r["category"] for r in rows))))
    rep.check("villes connues", all(r["city"] in CITIES for r in rows))
    rep.check("types numériques des coordonnées",
              all(isinstance(r["latitude"], float) and isinstance(r["longitude"], float) for r in rows))
    outside = [r["restaurant_id"] for r in rows
               if not (CITIES[r["city"]]["box"][0] <= r["latitude"] <= CITIES[r["city"]]["box"][1]
                       and CITIES[r["city"]]["box"][2] <= r["longitude"] <= CITIES[r["city"]]["box"][3])]
    rep.check("coordonnées dans l'emprise urbaine de leur ville", not outside, f"hors emprise : {outside[:5]}")
    rep.check("coordonnées au Maroc (lat 27..36, lon -13..-1)",
              all(27 <= r["latitude"] <= 36 and -13 <= r["longitude"] <= -1 for r in rows))
    rep.metrics["restaurants_by_city"] = dict(Counter(r["city"] for r in rows))
    rep.metrics["restaurants_by_category"] = dict(Counter(r["category"] for r in rows))
    print(f"  par ville     : {rep.metrics['restaurants_by_city']}")
    print(f"  par catégorie : {rep.metrics['restaurants_by_category']}")
    return {r["restaurant_id"]: r for r in rows}


# =============================================================================
def read_events(path: Path, fmt: str) -> list[tuple[dict, float | None, int | None]]:
    """Retourne [(événement, horodatage d'arrivée Kafka en s ou None, partition ou None)]."""
    out = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            continue
        if fmt == "jsonl":
            out.append((json.loads(raw), None, None))
            continue
        if not raw.startswith("CreateTime:"):
            continue  # lignes de service du consumer (ex. "Processed a total of ...")
        parts = raw.split("\t")
        created = partition = key = None
        for p in parts[:-1]:
            if p.startswith("CreateTime:"):
                created = int(p.split(":", 1)[1]) / 1000
            elif p.startswith("Partition:"):
                partition = int(p.split(":", 1)[1])
            elif not p.startswith("Offset:"):
                key = p
        ev = json.loads(parts[-1])
        if key is not None and key != ev.get("order_id"):
            raise ValueError(f"clé Kafka {key} != order_id {ev.get('order_id')}")
        out.append((ev, created, partition))
    return out


def validate_events(path: Path, fmt: str, restaurants: dict[str, dict], complete: bool,
                    expected_orders: int | None, rep: Report) -> None:
    print(f"\n=== Événements {path} (format={fmt}, commandes complètes attendues={complete})")
    records = read_events(path, fmt)
    events = [r[0] for r in records]
    n = len(events)
    rep.metrics["lines"] = n
    print(f"  {n} messages lus")

    # --- 1. Schéma -----------------------------------------------------------
    bad_fields = [e for e in events if list(e) != EVENT_FIELDS]
    rep.check("schéma : 10 champs exacts et ordonnés", not bad_fields, f"{len(bad_fields)} écarts")
    type_err = [e for e in events if not (
        isinstance(e["amount_mad"], float) and isinstance(e["items_count"], int)
        and isinstance(e["distance_km"], float) and all(isinstance(e[f], str) for f in
            ("order_id", "restaurant_id", "customer_id", "city", "status", "courier_id", "event_time")))]
    rep.check("types (str / float / int) conformes", not type_err, f"{len(type_err)} écarts")
    fmt_err = Counter(f for e in events for f, rx in RE.items() if not rx.match(e[f]))
    rep.check("formats (ORD-NNNNNNN, R-NNN, C-NNNNN, L-XXX-NN, ISO-8601 Z)", not fmt_err, str(dict(fmt_err)))
    rep.check("statuts dans {CREATED, PICKED_UP, DELIVERED, CANCELLED}", all(e["status"] in STATUSES for e in events))
    rep.check("bornes : amount_mad > 0, 1 <= items_count <= 10, 0 < distance_km <= 20",
              all(e["amount_mad"] > 0 and 1 <= e["items_count"] <= 10 and 0 < e["distance_km"] <= 20 for e in events))

    # --- 2. Cohérence référentielle -----------------------------------------
    unknown = {e["restaurant_id"] for e in events if e["restaurant_id"] not in restaurants}
    rep.check("restaurant_id présents dans restaurants.json", not unknown, f"inconnus : {sorted(unknown)[:5]}")
    city_mismatch = sum(1 for e in events if e["restaurant_id"] in restaurants
                        and restaurants[e["restaurant_id"]]["city"] != e["city"])
    rep.check("city = ville du restaurant", city_mismatch == 0, f"{city_mismatch} écarts")
    courier_mismatch = sum(1 for e in events if e["courier_id"][2:5] != e["city"][:3].upper())
    rep.check("code ville du livreur cohérent (L-CAS-.. à Casablanca)", courier_mismatch == 0, f"{courier_mismatch} écarts")

    # --- 3. Doublons : volontaires (contenu identique) vs collisions --------
    by_key: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for e in events:
        by_key[(e["order_id"], e["status"])].append(e)
    dup_extra = sum(len(v) - 1 for v in by_key.values() if len(v) > 1)
    collisions = [k for k, v in by_key.items() if len(v) > 1 and any(x != v[0] for x in v[1:])]
    rep.check("doublons = copies exactes (aucune collision d'identifiant)", not collisions, f"{len(collisions)} collisions")
    dup_rate = dup_extra / n if n else 0
    rep.metrics.update(duplicates=dup_extra, duplicate_rate=round(dup_rate, 4))

    # --- 4. Cohérence par commande : attributs stables + transitions ---------
    orders: dict[str, dict[str, dict]] = defaultdict(dict)
    for e in events:
        orders[e["order_id"]].setdefault(e["status"], e)
    unstable = [oid for oid, st in orders.items()
                if len({tuple(ev[f] for f in STATIC_FIELDS) for ev in st.values()}) > 1]
    rep.check("attributs de commande identiques dans tous ses événements", not unstable, f"{len(unstable)} commandes")
    def is_subsequence(seq: tuple, full: tuple) -> bool:
        it = iter(full)
        return all(s in it for s in seq)

    def seq_ok(seq: tuple) -> bool:
        if complete:
            return seq in VALID_COMPLETE
        # Flux capturé en cours : un statut en retard peut manquer
        return any(is_subsequence(seq, full) for full in VALID_COMPLETE)

    bad_seq, seq_counter = [], Counter()
    for oid, st in orders.items():
        seq = tuple(s for s, _ in sorted(st.items(), key=lambda kv: (ts(kv[1]["event_time"]),
                                                                   ["CREATED", "PICKED_UP", "DELIVERED", "CANCELLED"].index(kv[0]))))
        times = [ts(st[s]["event_time"]) for s in seq]
        strictly_increasing = all(a < b for a, b in zip(times, times[1:]))
        seq_counter[" -> ".join(seq)] += 1
        if not seq_ok(seq) or not strictly_increasing:
            bad_seq.append(oid)
    rep.check("transitions de statut valides et chronologiques", not bad_seq, f"{len(bad_seq)} commandes invalides")
    rep.metrics["sequences"] = dict(seq_counter.most_common())
    for s, c in seq_counter.most_common():
        print(f"      {c:>7}  {s}")
    rep.metrics["orders"] = len(orders)
    if expected_orders is not None:
        rep.check(f"nombre de commandes = {expected_orders}", len(orders) == expected_orders, f"{len(orders)} trouvées")
    nums = sorted(int(o[4:]) for o in orders)
    if complete and nums:
        rep.check("order_id contigus (aucun trou ni réutilisation)", nums == list(range(nums[0], nums[0] + len(nums))),
                  f"ORD-{nums[0]:07d} .. ORD-{nums[-1]:07d}")

    # --- 5. Retards ------------------------------------------------------------
    lags = []
    if fmt == "kafka-console":
        # Retard exact : horodatage Kafka d'arrivée (CreateTime) - event_time,
        # sur la première occurrence de chaque (order_id, status)
        first: dict[tuple[str, str], float] = {}
        for e, created, _ in records:
            key = (e["order_id"], e["status"])
            first[key] = min(first.get(key, created), created)
        lags = [created - ts(by_key[k][0]["event_time"]) for k, created in first.items()]
        rep.metrics["partitions"] = dict(sorted(Counter(p for _, _, p in records).items()))
        part_of = defaultdict(set)
        for e, _, p in records:
            part_of[e["order_id"]].add(p)
        rep.check("une commande = une seule partition (clé = order_id)", all(len(v) == 1 for v in part_of.values()))
        rep.check("les 3 partitions reçoivent des messages", len(rep.metrics["partitions"]) == 3,
                  str(rep.metrics["partitions"]))
    else:
        # Fichier : retard estimé = max(event_time déjà vu) - event_time, dans l'ordre d'arrivée
        seen = None
        first_seen = set()
        for e, _, _ in records:
            t = ts(e["event_time"])
            key = (e["order_id"], e["status"])
            if key in first_seen:
                continue  # le doublon n'est pas compté comme retard
            first_seen.add(key)
            lags.append(0.0 if seen is None else max(0.0, seen - t))
            seen = t if seen is None else max(seen, t)
    originals = len(lags)
    late = [l for l in lags if l > LATE_THRESHOLD_S]
    late_rate = len(late) / originals if originals else 0
    rep.metrics.update(late=len(late), late_rate=round(late_rate, 4),
                       late_over_10min=sum(1 for l in late if l > 600),
                       max_lag_min=round(max(lags) / 60, 2) if lags else 0)
    print(f"  retards (> {LATE_THRESHOLD_S}s) : {len(late)} / {originals} = {late_rate:.2%} ; "
          f"> 10 min : {rep.metrics['late_over_10min']} ; retard max = {rep.metrics['max_lag_min']} min")
    print(f"  doublons      : {dup_extra} / {n} = {dup_rate:.2%}")
    rep.check("retard maximal <= 15 min", not lags or max(lags) <= 15 * 60 + 1)
    if n >= 5000:  # proportions significatives seulement sur un volume suffisant
        rep.check("taux de retards ~5 % (3,5 % - 6,5 %)", 0.035 <= late_rate <= 0.065, f"{late_rate:.2%}")
        rep.check("taux de doublons ~2 % (1,4 % - 2,6 %)", 0.014 <= dup_rate <= 0.026, f"{dup_rate:.2%}")

    # --- 6. Statistiques métier (contrôle de vraisemblance) -------------------
    delivered = [e for e in {(e["order_id"], e["status"]): e for e in events}.values() if e["status"] == "DELIVERED"]
    if delivered:
        amounts = sorted(e["amount_mad"] for e in delivered)
        rep.metrics["delivered_amount_mad"] = {
            "min": amounts[0], "median": amounts[len(amounts) // 2], "max": amounts[-1],
            "mean": round(sum(amounts) / len(amounts), 2)}
        bands = Counter("court" if e["distance_km"] < 2 else "moyen" if e["distance_km"] <= 5 else "long" for e in delivered)
        rep.metrics["distance_bands_delivered"] = dict(bands)
        print(f"  montant livré (MAD) : {rep.metrics['delivered_amount_mad']}")
        print(f"  bandes de distance  : {dict(bands)}")
    times = [ts(e["event_time"]) for e in events]
    rep.metrics["event_time_range"] = [min(times), max(times)]
    print("  période : " + " -> ".join(datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                                        for t in (min(times), max(times))))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--restaurants", type=Path, required=True)
    p.add_argument("--events", type=Path)
    p.add_argument("--format", choices=["jsonl", "kafka-console"], default="jsonl")
    p.add_argument("--complete", action="store_true", help="toutes les commandes doivent être terminées")
    p.add_argument("--expected-orders", type=int)
    p.add_argument("--report", type=Path, help="écrit le rapport JSON à ce chemin")
    args = p.parse_args()

    rep = Report()
    restaurants = validate_restaurants(args.restaurants, rep)
    if args.events:
        validate_events(args.events, args.format, restaurants, args.complete, args.expected_orders, rep)
    failed = [c for c in rep.checks if not c[1]]
    print(f"\n=== BILAN : {len(rep.checks) - len(failed)}/{len(rep.checks)} contrôles OK"
          + ("" if not failed else " — ÉCHECS : " + "; ".join(c[0] for c in failed)))
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps({"checks": [{"name": n, "ok": o, "detail": d} for n, o, d in rep.checks],
                                           "metrics": rep.metrics}, indent=2, ensure_ascii=False), encoding="utf-8")
    sys.exit(0 if rep.ok else 1)


if __name__ == "__main__":
    main()
