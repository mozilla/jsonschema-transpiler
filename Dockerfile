FROM rust:latest

RUN apt-get update && apt-get install -y npm lsb-release software-properties-common

# llvm, npm, wasm-pack, and rustfmt
RUN curl https://apt.llvm.org/llvm.sh -sSf | bash
RUN npm install -g n && n stable
RUN curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh 
RUN rustup component add rustfmt

WORKDIR /app