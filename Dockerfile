FROM python:3.10-slim

WORKDIR /app

COPY . .

# Install dependencies
RUN pip install .

# Use ENTRYPOINT for the CLI
ENTRYPOINT ["databusclient"]
