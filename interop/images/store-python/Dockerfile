# syntax=docker/dockerfile:1
# The Python store-interop probe (examples/store-interop), and the one-shot RabbitMQ topology
# container. The source arrives as the named build context `pysrc` (compose.yaml): the branch or
# tag of https://github.com/snodra/snodra-messaging-python, or a local checkout (PYTHON_SRC).
# Base images are pinned by digest so the image is the same everywhere.
FROM python:3.12-slim-bookworm@sha256:44cf1793efa7c7a476bb0f2cff81443f4a072c90c1cb127e2924933daa3e8c64
COPY --from=ghcr.io/astral-sh/uv:0.12@sha256:3af4716e991d6956a41e573eab705d0ee08500cd829ed30293eb8472f372c65a /uv /bin/uv
# The interpreter of the base image is the only one: no download at build time.
ENV UV_COMPILE_BYTECODE=1 \
    UV_PYTHON=3.12 \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /src
# Only what the workspace build reads. A full checkout would bake in .git, .venv, bin, dist and
# the tool caches; the paths here are the workspace's manifests, its packages and its examples.
COPY --from=pysrc pyproject.toml uv.lock ./
COPY --from=pysrc packages ./packages
COPY --from=pysrc examples ./examples
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --package store-interop --no-dev
# The venv's own interpreter: nothing to sync at start, no network, no uv in the process tree.
ENTRYPOINT ["/src/.venv/bin/python", "-m", "store_interop"]
