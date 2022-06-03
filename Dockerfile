FROM python:3.10-slim
RUN useradd -u 10001 -m op
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY batchoperator batchoperator
USER 10001
ENTRYPOINT ["kopf", "run", "-m", "batchoperator.handlers"]
