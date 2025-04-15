FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
# Note: The actual code will be mounted as a volume in docker-compose.yml

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/
ENV DBT_PROFILES_DIR=/app/dbt

# Copy the entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Command to run when the container starts
CMD exec gunicorn --reload --workers 1 --bind 0.0.0.0:5000 index:app
