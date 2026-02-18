FROM python:3.12-slim

WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt \
    && python /app/scripts/generate-proto.py

ENV PROVIDER_CONFIG=/config/provider-config.yaml
EXPOSE 50051

CMD ["python", "server.py"]
