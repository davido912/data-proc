#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

function Usage() {
  printf "\t${RED} Invalid choice${NC}
	\tUsage: ./$(basename $0) --deploy/--destroy OR --deploy-rabbitmq/--destroy-rabbitmq \n "
}

if [ "$#" -ne 1 ]; then
  printf ${RED}'ERROR! You must provide only one argument!\n'${NC} >&2
  exit 1
fi

COMMAND=$1

function evalTests() {
  local TESTS_EXIT_CODE=${1}
  docker logs tests

  if [ -z ${TESTS_EXIT_CODE+x} ] || [ "$TESTS_EXIT_CODE" -ne 0 ]; then
    printf "${RED}Tests Failed${NC} - Exit Code: $TESTS_EXIT_CODE\n"
    exit 1
  else
    printf "${GREEN}Tests Passed${NC}\n"
  fi
}

if [[ $COMMAND == "--deploy" ]]; then
  docker-compose -f docker-compose.yaml -f docker-compose.tests.yaml up -d
  TESTS_EXIT_CODE=$(docker wait tests)
  evalTests TESTS_EXIT_CODE
  sleep 4 # the sleep here is just to have a few seconds to glance at the logs printed
  docker exec -it python-app /bin/bash /tmux.sh

elif [[ $COMMAND == "--deploy-rabbitmq" ]]; then
  docker-compose -f docker-compose-rabbitmq.yaml -f docker-compose-rabbitmq.tests.yaml up -d
  TESTS_EXIT_CODE=$(docker wait tests)
  evalTests TESTS_EXIT_CODE
  sleep 4 # the sleep here is just to have a few seconds to glance at the logs printed
  docker exec -it cli /bin/bash /tmux.sh rabbitmq

elif
  [[ $COMMAND == "--destroy-rabbitmq" ]]
then
  docker-compose -f docker-compose-rabbitmq.yaml -f docker-compose-rabbitmq.tests.yaml down
elif [[ $COMMAND == "--destroy" ]]; then
  docker-compose -f docker-compose.yaml -f docker-compose.tests.yaml down
else
  Usage
  exit 1
fi
