FROM python:3.9

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./app ./app

# Эти сервисы являются потребителями RabbitMQ.
# Они запускаются как воркеры.
CMD ["python", "main.py"]