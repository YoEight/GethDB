ARG TARGET_PLATFORM=linux/amd64
FROM --platform=$TARGET_PLATFORM rust:slim-bookworm AS build

WORKDIR /build
COPY ./geth-common/ ./geth-common
COPY ./geth-domain/ ./geth-domain
COPY ./geth-engine/ ./geth-engine
COPY ./geth-grpc/ ./geth-grpc
COPY ./geth-mikoshi/ ./geth-mikoshi
COPY ./geth-node/ ./geth-node

WORKDIR /build/.git
COPY ./.git/ .

RUN apt update && apt install -y protobuf-compiler

WORKDIR /build
RUN cargo install --path geth-node --root /geth

FROM build AS binaries

ENV GETH_HOST=0.0.0.0

ARG UID=1000
ARG GID=1000

WORKDIR /opt/gethdb

RUN addgroup --gid ${GID} "geth" && \
    adduser \
    --disabled-password \
    --gecos "" \
    --ingroup "geth" \
    --no-create-home \
    --uid ${UID} \
    "geth"

COPY --chown=geth:geth --from=build  ./geth/bin/* ./geth-node

RUN mkdir -p /var/lib/gethdb && \
    mkdir -p /var/log/gethdb && \
    mkdir -p /etc/gethdb && \
    chown -R geth:geth /var/lib/gethdb /var/log/gethdb /etc/gethdb

USER geth

VOLUME /var/lib/gethdb /var/log/gethdb

EXPOSE 2113/tcp

ENTRYPOINT [ "/opt/gethdb/geth-node" ]
CMD [ "--host", "0.0.0.0", "--db", "/var/lib/gethdb" ]
