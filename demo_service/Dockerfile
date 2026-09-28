FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir \
    fastapi==0.115.5 \
    "uvicorn[standard]==0.32.1" \
    pydantic==2.10.3 \
    confluent-kafka==2.6.1

COPY demo_service ./demo_service
EXPOSE 8000
CMD ["uvicorn", "demo_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
