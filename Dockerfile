# Local image only. CI does not build this Dockerfile.
FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
COPY scenarios ./scenarios
RUN pip install --no-cache-dir .

USER nobody
CMD ["python", "-m", "cdc_lab", "demo"]
