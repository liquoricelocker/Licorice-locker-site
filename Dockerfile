# Railway / Docker: no build-time secrets. Set STRIPE_SECRET_KEY, STRIPE_PUBLIC_KEY,
# STRIPE_WEBHOOK_SECRET, DATABASE_PATH (on the persistent volume),
# SITE_URL, etc. in Railway → Service → Variables (runtime only).
FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8080
EXPOSE 8080

CMD ["sh", "-c", "gunicorn --bind 0.0.0.0:$PORT app:app"]
