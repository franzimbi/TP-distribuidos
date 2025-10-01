#!/bin/bash
docker-compose -f docker-compose-dev.yaml stop -t 1
docker-compose -f docker-compose-mw.yaml stop -t 1
docker-compose -f docker-compose-dev.yaml down
docker-compose -f docker-compose-mw.yaml down

