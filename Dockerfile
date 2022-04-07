FROM ubuntu:21.10
CMD bash

ARG DEBIAN_FRONTEND=noninteractive
RUN sed -i 's/http:\/\/archive.ubuntu.com/http:\/\/mirror.sjtu.edu.cn/g' /etc/apt/sources.list && \
    apt -y update && \
    apt -y install \
        build-essential \
        cmake \
        libfmt-dev \
        git \
        clang-format \
        --no-install-recommends