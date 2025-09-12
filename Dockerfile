FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY main.go .
RUN go build -o /entrypoint main.go

FROM ubuntu:22.04

ARG BUCARDO_VERSION=5.6.0
ARG PG_VERSION=14

LABEL \
    maintainer="Wever Kley <wever-kley@live.com>" \
    org.opencontainers.image.title="Bucardo Docker Image" \
    org.opencontainers.image.description="An Ubuntu-based Docker image for Bucardo, a PostgreSQL replication system." \
    org.opencontainers.image.authors="Wever Kley <wever-kley@live.com>" \
    org.opencontainers.image.source="https://github.com/wever-kley/bucardo_docker_image" \
    org.opencontainers.image.documentation="https://github.com/wever-kley/bucardo_docker_image/blob/main/README.md" \
    org.opencontainers.image.licenses="Apache-2.0"

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Sao_Paulo

RUN apt-get -y update

RUN apt-get -y install postgresql-${PG_VERSION} jq wget curl perl make build-essential bucardo && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /tmp

RUN wget -O /tmp/bucardo.tgz https://github.com/bucardo/bucardo/archive/refs/tags/${BUCARDO_VERSION}.tar.gz && \
    tar zxf /tmp/bucardo.tgz && \
    cd bucardo-${BUCARDO_VERSION} && \
    perl Makefile.PL && \
    make && \
    make install && \
    cd / && \
    rm -rf /tmp/bucardo.tgz /tmp/bucardo-${BUCARDO_VERSION}

COPY etc/pg_hba.conf /etc/postgresql/${PG_VERSION}/main/
COPY etc/bucardorc /etc/bucardorc

RUN mkdir /var/run/bucardo 
RUN chown postgres /etc/postgresql/${PG_VERSION}/main/pg_hba.conf /etc/bucardorc /var/log/bucardo /var/run/bucardo
RUN usermod -aG postgres bucardo

RUN service postgresql start \
    && su - postgres -c "bucardo install --batch"

COPY --from=builder /entrypoint /entrypoint

WORKDIR /media/bucardo

ENTRYPOINT ["/entrypoint"]
CMD []
