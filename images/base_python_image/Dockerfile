FROM python:3.7-slim-buster

COPY requirements.txt /requirements.txt

RUN apt-get update -y -q  \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade  -y -q \
    && apt-get install -y  --no-install-recommends \
                vim \
                build-essential \
                gnupg2 \
                libpq-dev \
                && apt-get clean \
                && rm -rf /var/lib/apt/lists/*

RUN $(command -v pip) install --user --no-cache-dir -r /requirements.txt