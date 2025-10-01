FROM python:3.11-slim

WORKDIR /app

# Copy everything first (pyproject.toml, README.md, and source code)
COPY . .

# Install the package + dependencies
RUN pip install .

# Default command
ENTRYPOINT ["python", "-m", "databusclient"]
