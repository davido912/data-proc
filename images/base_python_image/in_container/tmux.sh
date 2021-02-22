#!/bin/bash
COMMAND=$1

bash -c 'while !</dev/tcp/postgres-db/5432; do sleep 2; done;'

if [[ $COMMAND == "rabbitmq" ]]; then
  tmux new-session -d -s rabbitmq-app
  tmux send-keys -t rabbitmq-app.0 'tmux split-window' ENTER
  tmux send-keys -t rabbitmq-app.0 'python /project/scripts/queue_implementation/cli.py' ENTER
  sleep 3 # sometimes tmux requires a bit of pause
  tmux send-keys -t rabbitmq-app.1 'psql postgres://dev:password@postgres-db:5432/events' ENTER
  tmux a -t rabbitmq-app

else
  tmux new-session -d -s python-app
  tmux send-keys -t python-app.0 'tmux split-window' ENTER
  tmux send-keys -t python-app.0 'python /project/scripts/main.py' ENTER
  sleep 3 # sometimes tmux requires a bit of pause
  tmux send-keys -t python-app.1 'psql postgres://dev:password@postgres-db:5432/events' ENTER
  tmux a -t python-app

fi
