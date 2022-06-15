FROM alpine:latest as builder

ENV GOROOT=/usr/lib/go
ENV PATH="${GOROOT}/bin:${PATH}"
RUN apk add git go
WORKDIR /usr/src
RUN git clone https://github.com/graygnuorg/mercure.git
RUN cd mercure/caddy && \
    go mod tidy && \
    cd mercure && \
    go build

FROM caddy:latest
ENV MERCURE_TRANSPORT_URL=bolt:///data/mercure.db
COPY --from=builder /usr/src/mercure/caddy/mercure/mercure /usr/bin/caddy
COPY --from=builder /usr/src/mercure/Caddyfile /etc/caddy/Caddyfile
COPY --from=builder /usr/src/mercure/Caddyfile.dev /etc/caddy/Caddyfile.dev
