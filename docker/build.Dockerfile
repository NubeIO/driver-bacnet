FROM ubuntu:18.04 as base

RUN apt update -qq \
    && apt install build-essential net-tools git libssl-dev cmake -y

COPY bacnet-stack /opt/bacnet-stack

RUN mkdir -p /opt/mqtt && cd /opt/mqtt \
    && git clone https://github.com/eclipse/paho.mqtt.c paho.mqtt.c \
    && cd /opt/mqtt/paho.mqtt.c && cmake -DPAHO_BUILD_STATIC=TRUE && make install \
    && apt-get install libyaml-dev -y \
    && mkdir -p /opt/cyaml && cd /opt/cyaml \
    && git clone https://github.com/tlsa/libcyaml.git \
    && cd /opt/cyaml/libcyaml \
    && make install VARIANT=release

RUN find / -name libyaml.a -print -exec cp -fR {} /usr/lib \;

RUN cd /opt/bacnet-stack \
    && make clean all

WORKDIR /

RUN mkdir -p /opt/bacnet-stack/app/ \
    && cp /opt/bacnet-stack/bin/bacserv /opt/bacnet-stack/app

