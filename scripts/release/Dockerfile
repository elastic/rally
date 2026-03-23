# Image for scripts/release/prepare-docker.sh: Python plus jq, uv, and tooling
# required by scripts/release/prepare.sh and the repo pre-commit hook (make pre-commit).
# Pinned to 3.13; CI still tests 3.10–3.13 (see scripts/release/prepare-docker.sh header).
FROM python:3.13-bookworm

RUN apt-get update \
	&& DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
		build-essential \
		curl \
		git \
		jq \
		make \
	&& rm -rf /var/lib/apt/lists/*

# Keep in sync with docker/Dockerfiles/dev/Dockerfile
COPY --from=ghcr.io/astral-sh/uv:0.8.22 /uv /uvx /bin/

# pip / github3.py: canonical versions live in pyproject.toml ([project].dependencies pip,
# [project.optional-dependencies].develop github3.py). uv.lock follows pyproject; bump all
# together when changing. prepare-docker.sh pins github3.py again for the bind-mounted .venv.
RUN pip install "pip==26.0.1"
RUN pip install "github3.py==3.2.0"

# Editable install: copy only what the build needs so unrelated changes (tests,
# docs, scripts, …) do not invalidate the pip layer. Build context is repo root
# (see prepare-docker.sh).
WORKDIR /workspace
COPY pyproject.toml README.md ./
COPY esrally ./esrally/
RUN pip install --editable .
