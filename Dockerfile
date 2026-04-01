FROM elastic/rally:2.12.0 AS rally

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

COPY pyproject.toml /src/pyproject.toml
COPY esrally/_version.py /src/esrally/_version.py
COPY esrally/__init__.py /src/esrally/__init__.py
COPY README.md /src/README.md
RUN uv pip install --upgrade /src/

COPY . /src
RUN uv pip install --upgrade /src/

ENTRYPOINT ["esrally"]
CMD ["--help"]
