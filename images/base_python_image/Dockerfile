FROM python:3.7.10-slim-buster

COPY requirements.txt /requirements.txt
COPY /in_container/tmux.sh /tmux.sh

ADD scripts /project/scripts
ADD raw_data /project/raw_data

RUN apt-get update -y -q  \
    && apt-get install -y  \
                libpq-dev \
                gcc\
                vim \
                tmux \
                postgresql-client \
                && apt-get clean \
                && rm -rf /var/lib/apt/lists/*

RUN $(command -v pip) install --user --no-cache-dir -r /requirements.txt