FROM alpine:3.14

WORKDIR /dataset
RUN apk add --no-cache git make gcc bash libc-dev
COPY scripts/data-gen.sh ./scripts/data-gen.sh
RUN git clone https://github.com/gregrahn/tpch-kit.git
WORKDIR /dataset/tpch-kit/dbgen
RUN make MACHINE=LINUX DATABASE=POSTGRESQL

WORKDIR /dataset/scripts/
CMD ["/bin/bash"]
