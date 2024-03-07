# syntax=docker/dockerfile:1

FROM python:3.9-slim as python-base

# Install necessary system dependencies
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        curl \
        build-essential
        
################################
# BUILDER-BASE
# Used to build deps + create our virtual environment
################################
FROM python-base as builder-base
RUN pip install hatch
ENV PYSETUP_PATH="/opt/pysetup" \
    VENV_PATH="/opt/pysetup/.venv"

COPY pyproject.toml ./
ARG optional_dependencies

RUN hatch dep show requirements >> requirements.txt
RUN python -m pip install -r requirements.txt

RUN if [ -n "$optional_dependencies" ]; then \
        echo '#!/bin/bash\n\
            IFS=',' read -ra optional_dependencies <<< "$1"\n\
            for DEP in "${optional_dependencies[@]}"; do\n\
                hatch dep show requirements --feature "$DEP" >> requirements_optional.txt\n\
            done\n\
            python -m pip install -r requirements_optional.txt' > create_and_install_optional_requirements.sh; \
        chmod +x create_and_install_optional_requirements.sh; \
        ./create_and_install_optional_requirements.sh "$optional_dependencies"; \
    else \
        echo "No optional dependencies specified."; \
    fi


################################
# PRODUCTION
# Final image used for runtime
################################
FROM python-base as production
COPY --from=builder-base $PYSETUP_PATH $PYSETUP_PATH
COPY ./src /app
WORKDIR /app
ENTRYPOINT ["python", "-m", "morph_kgc"]
