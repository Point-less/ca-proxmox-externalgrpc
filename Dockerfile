FROM python:3.12-alpine

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY source/ /app/
RUN python /app/scripts/generate-proto.py

ENV PROVIDER_CONFIG=/config/provider-config.yaml
EXPOSE 50051

CMD ["python", "-m", "app.server"]
