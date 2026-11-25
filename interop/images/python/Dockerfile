# syntax=docker/dockerfile:1
# The Python conformance service of snodra-messaging-python (examples/conformance-service).
# The source arrives as the named build context `pysrc` (compose.yaml): the main branch of
# https://github.com/snodra/snodra-messaging-python, or a local checkout (PYTHON_SRC).
# Base images are pinned by digest so the image is the same everywhere.
FROM python:3.12-slim-bookworm@sha256:44cf1793efa7c7a476bb0f2cff81443f4a072c90c1cb127e2924933daa3e8c64
COPY --from=ghcr.io/astral-sh/uv:0.12@sha256:3af4716e991d6956a41e573eab705d0ee08500cd829ed30293eb8472f372c65a /uv /bin/uv
# The interpreter of the base image is the only one: no download, no hard links across layers.
# UV_PYTHON selects it even though the checkout's .python-version asks for another version.
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_DEV=1 \
    UV_PYTHON=3.12 \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /src
COPY --from=pysrc . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --package snodra-conformance-service --no-dev
# The service registers every conformance type on each hub and stops on SIGTERM within 5 s.
ENTRYPOINT ["uv", "run", "--frozen", "--package", "snodra-conformance-service", "python", "-m", "conformance_service"]
