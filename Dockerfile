# syntax=docker/dockerfile:1

FROM python:3.9-slim as python-base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.3.2 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PYSETUP_PATH="/opt/pysetup" \
    VENV_PATH="/opt/pysetup/.venv" 

ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

# syntax=docker/dockerfile:1
FROM python-base as builder-base
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        curl \
        build-essential 
RUN apt-get install -y freetds-dev libkrb5-dev libssl-dev libffi-dev libgssapi-krb5-2

RUN --mount=type=cache,target=/root/.cache \
    curl -sSL https://install.python-poetry.org | python3 -

WORKDIR $PYSETUP_PATH
COPY poetry.lock pyproject.toml ./
RUN poetry remove pyoxigraph
ARG optional_dependencies
RUN --mount=type=cache,target=/root/.cache \
    poetry install --extras "$optional_dependencies"
RUN pip install pyoxigraph
FROM python-base as development
WORKDIR /app
COPY --from=builder-base $POETRY_HOME $POETRY_HOME
COPY --from=builder-base $PYSETUP_PATH $PYSETUP_PATH
RUN --mount=type=cache,target=/root/.cache \
    poetry install --with=dev

EXPOSE 8000
CMD ["python", "-m", "morph_kgc", "files/config.ini"]

FROM python-base as production
COPY --from=builder-base $PYSETUP_PATH $PYSETUP_PATH
COPY /src /app/
WORKDIR /app
CMD ["python", "-m", "morph_kgc", "files/config.ini"]