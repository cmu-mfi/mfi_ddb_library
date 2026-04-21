FROM python:3.12-slim

RUN apt update && apt install -y git

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN git clone --recurse-submodules https://github.com/cmu-mfi/mfi_ddb_library.git && \
    cd mfi_ddb_library && \
    uv pip install --system .