#!/bin/bash
docker-compose -f docker-compose-test.yaml stop -t 1
docker-compose -f docker-compose-test.yaml down --remove-orphans
