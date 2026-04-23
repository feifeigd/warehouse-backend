# 已经包含 vcpkg
FROM mcr.microsoft.com/devcontainers/cpp:1-ubuntu-24.04

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y \
    && apt-get install -y --no-install-recommends \
        autoconf autoconf-archive automake \
        build-essential \
        ca-certificates \
        ccache \
        clang \
        clang-format \
        clang-tidy \
        clangd \
        cmake \
        cppcheck \
        curl \
        gdb \
        git \
        libtool \
        linux-libc-dev \
        lld \
        lldb \
        m4 \
        ninja-build \
        pkg-config \
        python3 \
        python3-pip \
        tar \
        unzip \
        valgrind \
        vim \
        zip \
    && rm -rf /var/lib/apt/lists/*
    
ENV CC=gcc
ENV CXX=g++
ENV VCPKG_ROOT=/usr/local/vcpkg
ENV VCPKG_FEATURE_FLAGS=manifests

# Set a sane default for CTest to show output on failure
# ENV CTEST_OUTPUT_ON_FAILURE=ON
