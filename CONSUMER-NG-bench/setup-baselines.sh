#!/bin/bash
# Build the comparison consumers on morefine without root: Go (tarball) + franz-go bench, librdkafka (mklove) + rdkafka_performance.
set -u
mkdir -p ~/baselines && cd ~/baselines
if [ ! -x ~/go/bin/go ]; then
  curl -sSL https://go.dev/dl/go1.25.4.linux-amd64.tar.gz -o go.tgz && rm -rf ~/go && tar -C ~ -xzf go.tgz && echo "go installed"
fi
export PATH=$HOME/go/bin:$PATH GOPATH=$HOME/gopath GOFLAGS=-mod=mod
go version
if [ ! -d franz-go ]; then git clone -q --depth 1 https://github.com/twmb/franz-go.git && echo "franz-go cloned"; fi
(cd franz-go/examples/bench && go build -o ~/baselines/franz-bench . && echo "franz-bench built") 2>&1 | tail -3
if [ ! -d librdkafka ]; then git clone -q --depth 1 https://github.com/confluentinc/librdkafka.git && echo "librdkafka cloned"; fi
(cd librdkafka && ./configure --disable-zlib --disable-zstd --disable-curl --disable-gssapi --disable-sasl --disable-ssl > ../librdkafka-configure.log 2>&1 && make -j4 libs > ../librdkafka-make.log 2>&1 && make -C examples rdkafka_performance >> ../librdkafka-make.log 2>&1 && cp examples/rdkafka_performance ~/baselines/ && echo "rdkafka_performance built") 2>&1 | tail -3
ls -la ~/baselines | grep -E "franz-bench|rdkafka_performance"
