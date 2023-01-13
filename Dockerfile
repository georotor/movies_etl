FROM python:3.10-slim

WORKDIR /opt/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY etl/requirements.txt requirements.txt

RUN apt-get update && apt-get install --no-install-recommends -y \
     curl \
     gcc \
     libc6-dev \
     netcat \
     \
     && pip install --no-cache-dir --upgrade pip \
     && pip install --no-cache-dir -r requirements.txt \
     \
     && apt-get remove -y gcc \
     && apt autoremove -y \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/*

COPY etl/ /opt/app/

RUN groupadd -r bob && useradd -d /opt/app -r -g bob bob \
     && chown bob:bob -R /opt/app

USER bob

ENTRYPOINT ["/opt/app/entrypoint.sh"]

