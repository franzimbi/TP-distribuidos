docker-compose -f docker-compose-mw.yaml up --build -d

# esperar a rabbit
echo "\nEsperando a que RabbitMQ esté listo...\n"
sleep 15

# levantar tp
docker-compose -f docker-compose-dev.yaml up --build

# logs en tiempo real de tp solamente
#docker-compose -f docker-compose-dev.yaml logs -f