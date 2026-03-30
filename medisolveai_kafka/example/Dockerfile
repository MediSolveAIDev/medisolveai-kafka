FROM python:3.13-slim

WORKDIR /app

COPY ../.. /app

RUN pip install --no-cache-dir .[dev] fastapi uvicorn[standard] python-dotenv websockets

CMD ["python", "medisolveai_kafka/example/app.py"]
