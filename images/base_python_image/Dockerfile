FROM python:3.7.10-slim-buster

COPY /in_container/requirements.txt /requirements.txt
COPY /in_container/pytest.ini /project/pytest.ini
COPY /in_container/tmux.sh /tmux.sh
COPY /in_container/wait_for_rabbitmq.sh /wait_for_rabbitmq.sh

ADD scripts /project/scripts
ADD raw_data /project/raw_data

RUN apt-get update -y -q  \
    && apt-get install -y  \
                libpq-dev \
                gcc\
                vim \
                tmux \
                curl \
                postgresql-client \
                && apt-get clean \
                && rm -rf /var/lib/apt/lists/*

RUN $(command -v pip) install --user --no-cache-dir -r /requirements.txt

WORKDIR /project