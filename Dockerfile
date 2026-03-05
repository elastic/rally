# -----------------------------------------------------------------------------
# Stage: Rally image + Python TCP proxy + gor (sidecar)
# -----------------------------------------------------------------------------
FROM elastic/rally:2.12.0 AS rally

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

COPY . /src
RUN uv tool install /src/

CMD ["esrally"]
