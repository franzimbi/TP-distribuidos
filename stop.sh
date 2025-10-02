#!/bin/bash
docker-compose -f docker-compose-dev.yaml stop -t 1
docker-compose -f docker-compose-mw.yaml stop -t 1
docker-compose -f docker-compose-dev.yaml down --remove-orphans
docker-compose -f docker-compose-mw.yaml down --remove-orphans

