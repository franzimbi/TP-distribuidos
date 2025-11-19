CARPETA="backups"
if [ -d "$CARPETA" ]; then
    echo "Borrando carpeta $CARPETA..."
    rm -rf "$CARPETA"
fi

find . -maxdepth 1 -type d -name "results_cliente_?" -exec rm -rf {} +

docker-compose -f docker-compose-mw.yaml up --build -d

# esperar a rabbit
echo "\nEsperando a que RabbitMQ esté listo...\n"
sleep 20

# levantar tp
docker-compose -f docker-compose-dev.yaml up --build

# logs en tiempo real de tp solamente
#docker-compose -f docker-compose-dev.yaml logs -f