FROM python:3.12-slim
ARG COMMIT_HASH
ARG BUILD_TAG
ENV COMMIT_HASH=$COMMIT_HASH
ENV BUILD_TAG=$BUILD_TAG

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"] 