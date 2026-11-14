#!/usr/bin/env python3
"""
=============================================================================
 RECONSTITUTION — ce script N'EST PAS un fichier fourni par le professeur.
 Les fichiers originaux (restaurants.json, producer.py) étaient absents ;
 ce générateur reconstitue restaurants.json d'après le §2.1 du sujet :
   « Référentiel restaurants (fichier statique restaurants.json, 200 lignes) :
     identifiant, nom, ville, catégorie (pizza, tacos, sushi, marocain…),
     latitude, longitude. »
=============================================================================

Format de sortie : JSON Lines (un objet par ligne, 200 lignes), lisible
directement par spark.read.json() sans l'option multiLine.

    {"restaurant_id": "R-001", "name": "...", "city": "Casablanca",
     "category": "pizza", "latitude": 33.5612, "longitude": -7.6311}

Usage :
    python3 data/generate_restaurants.py [--seed 42] [--output data/restaurants.json]

Déterministe : même graine => fichier identique octet pour octet.
"""
from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

# ---------------------------------------------------------------------------
# Villes marocaines : nombre de restaurants (total = 200) et emprise urbaine
# terrestre approximative (lat_min, lat_max, lon_min, lon_max) en WGS84.
# Les emprises sont volontairement resserrées sur le centre urbain pour que
# les points ne tombent pas en mer (villes côtières : Casablanca, Rabat,
# Tanger, Agadir). Noms en ASCII : ils servent de partitions S3 (city=Fes).
# ---------------------------------------------------------------------------
CITIES: dict[str, dict] = {
    "Casablanca": {"n": 50, "box": (33.530, 33.585, -7.660, -7.520),
                   "areas": ["Maarif", "Gauthier", "Ain Diab", "Bourgogne", "Anfa",
                             "Sidi Maarouf", "Hay Hassani", "Derb Sultan", "Oasis", "Belvedere"]},
    "Rabat":      {"n": 30, "box": (33.950, 34.010, -6.870, -6.810),
                   "areas": ["Agdal", "Hassan", "Hay Riad", "Souissi", "Ocean", "Akkari"]},
    "Marrakech":  {"n": 30, "box": (31.590, 31.665, -8.050, -7.960),
                   "areas": ["Gueliz", "Hivernage", "Medina", "Sidi Ghanem", "Targa", "Daoudiate"]},
    "Fes":        {"n": 22, "box": (33.990, 34.060, -5.030, -4.960),
                   "areas": ["Ville Nouvelle", "Fes el Bali", "Saiss", "Narjiss", "Atlas"]},
    "Tanger":     {"n": 22, "box": (35.725, 35.775, -5.860, -5.800),
                   "areas": ["Malabata", "Centre", "Marshan", "Iberia", "Boukhalef"]},
    "Agadir":     {"n": 18, "box": (30.390, 30.440, -9.580, -9.530),
                   "areas": ["Talborjt", "Founty", "Hay Mohammadi", "Dakhla", "Charaf"]},
    "Meknes":     {"n": 14, "box": (33.865, 33.915, -5.570, -5.510),
                   "areas": ["Hamria", "Ville Nouvelle", "Medina", "Marjane"]},
    "Oujda":      {"n": 14, "box": (34.665, 34.700, -1.935, -1.885),
                   "areas": ["Centre", "Lazaret", "Al Qods", "Sidi Yahya"]},
}

# Catégories et poids de tirage (marocain et pizza dominent l'offre locale)
CATEGORIES: dict[str, dict] = {
    "marocain":   {"w": 0.22, "names": ["Dar", "Riad", "Chez", "La Table", "Le Tajine", "Bab"],
                   "suffix": ["Zitoun", "Yasmine", "Andalous", "Saada", "Nour", "Atlas", "Mogador", "Fassia"]},
    "pizza":      {"w": 0.18, "names": ["Pizza", "Pizzeria", "Forno"],
                   "suffix": ["Napoli", "Bella", "Roma", "Del Sol", "Vesuvio", "Mamma"]},
    "tacos":      {"w": 0.14, "names": ["Tacos", "Le Tacos", "Taco"],
                   "suffix": ["Express", "Factory", "House", "Corner", "Street"]},
    "burger":     {"w": 0.12, "names": ["Burger", "Smash", "Grill"],
                   "suffix": ["Station", "Club", "Lab", "Spot", "Brothers"]},
    "sushi":      {"w": 0.10, "names": ["Sushi", "Maki", "Sakura"],
                   "suffix": ["Bar", "Time", "Zen", "Kyoto", "Wave"]},
    "shawarma":   {"w": 0.10, "names": ["Shawarma", "Damas", "Beyrouth"],
                   "suffix": ["Palace", "Express", "Grill", "Corner"]},
    "healthy":    {"w": 0.07, "names": ["Green", "Fresh", "Bowl"],
                   "suffix": ["Garden", "Kitchen", "Factory", "Life"]},
    "patisserie": {"w": 0.07, "names": ["Patisserie", "Maison", "Douceurs"],
                   "suffix": ["Amandine", "Chebakia", "Gazelle", "Sablee", "Miel"]},
}


def generate(seed: int) -> list[dict]:
    rng = random.Random(seed)
    cats = list(CATEGORIES)
    weights = [CATEGORIES[c]["w"] for c in cats]
    restaurants: list[dict] = []
    used_names: set[str] = set()
    rid = 1
    for city, spec in CITIES.items():
        lat_min, lat_max, lon_min, lon_max = spec["box"]
        for _ in range(spec["n"]):
            cat = rng.choices(cats, weights=weights)[0]
            c = CATEGORIES[cat]
            # Nom unique : "<enseigne> <suffixe> <quartier>"
            for _attempt in range(100):
                name = f"{rng.choice(c['names'])} {rng.choice(c['suffix'])} {rng.choice(spec['areas'])}"
                if name not in used_names:
                    break
            else:  # pragma: no cover - très improbable
                name = f"{name} {rid}"
            used_names.add(name)
            restaurants.append({
                "restaurant_id": f"R-{rid:03d}",
                "name": name,
                "city": city,
                "category": cat,
                "latitude": round(rng.uniform(lat_min, lat_max), 6),
                "longitude": round(rng.uniform(lon_min, lon_max), 6),
            })
            rid += 1
    return restaurants


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconstitution de restaurants.json (200 restaurants marocains).")
    parser.add_argument("--seed", type=int, default=42, help="graine aléatoire (défaut : 42)")
    parser.add_argument("--output", type=Path, default=Path(__file__).with_name("restaurants.json"),
                        help="fichier de sortie JSON Lines (défaut : data/restaurants.json)")
    args = parser.parse_args()

    rows = generate(args.seed)
    assert len(rows) == 200, len(rows)
    with args.output.open("w", encoding="utf-8", newline="\n") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"{len(rows)} restaurants écrits dans {args.output} (seed={args.seed})")


if __name__ == "__main__":
    main()
