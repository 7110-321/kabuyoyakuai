FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8080
CMD ["bash", "-lc", "exec gunicorn -b 0.0.0.0:${PORT:-8080} main:app"]
