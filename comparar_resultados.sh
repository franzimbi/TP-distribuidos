#!/bin/bash

DIR="results_cliente_1"
DIR2="results_full"

GREEN="\033[0;32m"
RED="\033[0;31m"
NC="\033[0m"

for f in "$DIR"/q*.csv; do
    echo "Ordenando $f..."
    sort "$f" -o "$f"
done

for f in "$DIR2"/q*.csv; do
    sort "$f" -o "$f"
done

for f1 in "$DIR"/q*.csv; do
    file=$(basename "$f1")
    f2="$DIR2/$file"

    if [[ -f "$f2" ]]; then
        echo "Comparando $file..."

       if diff -q "$f1" "$f2" > /dev/null; then
            echo -e "${GREEN}No hay diferencias${NC}"
        else
            echo -e "${RED}Hay diferencias${NC}"
        fi

        echo "---------------------------------------"
    else
        echo "El archivo $file no existe en $DIR2"
    fi
done