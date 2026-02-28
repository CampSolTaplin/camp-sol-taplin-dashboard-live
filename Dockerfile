FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (cached layer — only rebuilds when requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code (fast — only this layer rebuilds on code changes)
COPY . .

# Render injects PORT env var
CMD ["gunicorn", "app:app", "--timeout", "120", "--bind", "0.0.0.0:10000"]
