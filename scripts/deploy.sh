#!/usr/bin/env bash
# Despliega este repo en el host de Airflow. Lo ejecuta el runner self-hosted después de que CI valide.
#
# Uso: scripts/deploy.sh <commit_anterior> <commit_actual>
#
# - Cambios solo en dags/: se sincronizan y el dag-processor los recoge en su siguiente ciclo. Nada se reinicia.
# - Cambios en la infraestructura (compose, Dockerfile, requirements): se reconstruye la imagen y se recrean
#   los contenedores, pero solo si el stack está corriendo. Si está apagado a propósito, sigue apagado.
set -euo pipefail

AIRFLOW_DIR="${AIRFLOW_DIR:-/home/github_runner/airflow}"
BEFORE="${1:-}"
AFTER="${2:-HEAD}"
INFRA_FILES=(docker-compose.yaml Dockerfile requirements.txt)

# 1. Sin secretos no se despliega nada: compose se negaría a arrancar y Airflow guardaría credenciales en claro.
for var in FERNET_KEY JWT_SECRET; do
  if ! grep -qE "^${var}=.+" "$AIRFLOW_DIR/.env" 2>/dev/null; then
    echo "::error::Falta ${var} en $AIRFLOW_DIR/.env. Ver .env.example."
    exit 1
  fi
done

# 2. ¿Qué cambió? Sin un commit anterior utilizable (primer push, force-push) se asume que todo.
if [[ -n "$BEFORE" && ! "$BEFORE" =~ ^0+$ ]] && git cat-file -e "${BEFORE}^{commit}" 2>/dev/null; then
  changed=$(git diff --name-only "$BEFORE" "$AFTER")
else
  changed=$(git ls-files)
fi

infra_changed=false
for f in "${INFRA_FILES[@]}"; do
  if grep -qxF "$f" <<<"$changed"; then
    infra_changed=true
  fi
done

# 3. Sincronizar. dags/ con --delete porque el repo es la fuente de verdad; la infra sin tocar .env, logs ni config.
rsync -av --delete --exclude='__pycache__/' dags/ "$AIRFLOW_DIR/dags/"
rsync -av "${INFRA_FILES[@]}" "$AIRFLOW_DIR/"

if [[ "$infra_changed" != true ]]; then
  echo "Solo cambiaron DAGs: el dag-processor los recoge en su próximo ciclo."
  exit 0
fi

# 4. Aplicar la infraestructura nueva, solo si el stack ya estaba corriendo.
cd "$AIRFLOW_DIR"
if [[ -z "$(docker compose ps --status running --quiet airflow-scheduler)" ]]; then
  echo "::notice::Cambió la infraestructura pero el stack está detenido. Se aplicará con: docker compose up -d --build"
  exit 0
fi

docker compose build
# --remove-orphans elimina contenedores de servicios que ya no existen en el compose (p. ej. redis y el worker).
docker compose up -d --remove-orphans
