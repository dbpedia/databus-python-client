FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /data

COPY . .

# Install dependencies
RUN pip install .

# Use ENTRYPOINT for the CLI
ENTRYPOINT ["databusclient"]
