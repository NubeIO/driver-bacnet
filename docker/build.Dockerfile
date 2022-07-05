FROM ubuntu:18.04 as base

RUN apt update -qq \
    && apt install build-essential net-tools git libssl-dev -y

COPY bacnet-stack /opt/bacnet-stack

RUN mkdir -p /opt/mqtt && cd /opt/mqtt \
    && git clone https://github.com/eclipse/paho.mqtt.c paho.mqtt.c \
    && cd /opt/mqtt/paho.mqtt.c && make install

RUN cd /opt/bacnet-stack \
    && make clean all

WORKDIR /

RUN mkdir -p /opt/bacnet-stack/app/ \
    && cp /opt/bacnet-stack/bin/bacserv /opt/bacnet-stack/app

