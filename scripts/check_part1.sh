#!/usr/bin/env bash
# =============================================================================
# Partie 1 — collecte des preuves de fonctionnement de l'infrastructure.
# Usage (depuis la racine du projet, Git Bash ou Linux) : bash scripts/check_part1.sh
# Lecture seule : aucune ressource n'est créée ni supprimée.
# =============================================================================
set -u
cd "$(dirname "$0")/.."
# Git Bash (Windows) : empêche la conversion des chemins /opt/... en C:/Program Files/Git/opt/...
export MSYS_NO_PATHCONV=1

# Interpréteur Python de l'hôte (python3 sous Linux, python sous Windows)
PY=python3; "$PY" -c 1 2>/dev/null || PY=python

section() { printf '\n==================== %s ====================\n' "$1"; }

section "État des services (docker compose ps)"
docker compose ps -a --format 'table {{.Service}}\t{{.State}}\t{{.Status}}\t{{.Ports}}'

section "Kafka — topic orders"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --describe --topic orders

section "MinIO — bucket lake (mc ls --recursive local/lake/)"
docker compose exec -T minio sh -c \
  'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc ls --recursive local/lake/'

section "Spark Master — workers enregistrés (API JSON :8080/json)"
docker compose exec -T spark-master sh -c 'wget -q -O- http://localhost:8080/json/' | "$PY" -c '
import json, sys
d = json.load(sys.stdin)
print("Master : {url}  status={status}".format(**d))
print("Workers ALIVE : {aliveworkers}   cores={cores}   memoire={memory} Mo".format(**d))
for w in d["workers"]:
    print("  - {host:<16} {state:<6} cores={cores} memory={memory} Mo  ui={webuiaddress}".format(**w))
'

section "Interfaces web (code HTTP depuis l'hôte)"
for url in http://localhost:8080 http://localhost:8181 http://localhost:8182 \
           http://localhost:18080 http://localhost:9001 http://localhost:8888/api; do
  printf '  %-28s %s\n' "$url" "$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$url")"
done
