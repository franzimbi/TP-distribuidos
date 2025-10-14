#!/bin/bash

if [ $# -lt 2 ]; then
  echo "Uso: $0 <cantidad_nodos> <cantidad_clientes>"
  exit 1
fi

echo "Cantidad de filtros: $1"
echo "Cantidad de aggregators: $2"
echo "Cantidad de joins: $3"
echo "Cantidad de clientes: $4"
echo "Generando docker-compose.yml..."

# Verificar si PyYAML está instalado
if ! python3 -c "import yaml" &> /dev/null; then
  echo "[INFO] PyYAML no está instalado. Instalando..."
  pip3 install pyyaml --quiet
  if [ $? -ne 0 ]; then
    echo "[ERROR] No se pudo instalar PyYAML. Verifica tu entorno o conexión a Internet."
    exit 1
  fi
  echo "[INFO] PyYAML instalado correctamente."
fi

# Ejecutar el generador
python3 generator.py "$1" "$2" "$3" "$4"