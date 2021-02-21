#!/usr/bin/env bash

COMMAND=$1

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

if [[ $COMMAND == "deploy" ]]; then
  docker-compose -f docker-compose.yaml -f docker-compose.tests.yaml up -d
  TESTS_EXIT_CODE=$(docker wait tests)
  docker logs tests

  if [ -z ${TESTS_EXIT_CODE+x} ] || [ "$TESTS_EXIT_CODE" -ne 0 ]; then
    printf "${RED}Tests Failed${NC} - Exit Code: $TESTS_EXIT_CODE\n"
  else
    printf "${GREEN}Tests Passed${NC}\n"
    sleep 4 # the sleep here is just to have a few seconds to glance at the logs printed
    docker exec -it python-app /bin/bash /tmux.sh
  fi
elif [[ $COMMAND == "destroy" ]]; then
  docker-compose -f docker-compose.yaml -f docker-compose.tests.yaml down
else
  echo "Invalid command, available commands are deploy and destroy"
  exit 1
fi
