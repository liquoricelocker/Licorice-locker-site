# Railway / Docker: no build-time secrets. Set STRIPE_SECRET_KEY, STRIPE_PUBLIC_KEY,
# SITE_URL, etc. in Railway → Service → Variables (runtime only).
FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8000
EXPOSE 8000

CMD gunicorn app:app --bind 0.0.0.0:${PORT}
