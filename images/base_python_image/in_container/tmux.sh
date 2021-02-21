#!/bin/bash
bash -c 'while !</dev/tcp/postgres-db/5432; do sleep 2; done;'
tmux new-session -d -s python-app;
tmux send-keys -t python-app.0 'tmux split-window' ENTER
tmux send-keys -t python-app.0 'python /project/scripts/main.py' ENTER
sleep 3 # sometimes tmux requires a bit of pause
tmux send-keys -t python-app.1 'psql postgres://dev:password@postgres-db:5432/events' ENTER
tmux a -t python-app