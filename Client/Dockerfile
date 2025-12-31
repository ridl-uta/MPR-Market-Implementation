FROM python:3.9-slim

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

# Ensure Python output is unbuffered for timely logging
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python3", "main.py"]
