FROM python:3.12-slim

WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt
RUN python /app/source/scripts/generate-proto.py

ENV PROVIDER_CONFIG=/config/provider-config.yaml
EXPOSE 50051

CMD ["python", "-m", "source.server"]
