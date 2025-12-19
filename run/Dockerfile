# Use a base Python image
FROM python:3.10-slim

# Install system dependencies (if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Copy the training script
COPY train.py .

# Install requirements
RUN pip install --no-cache-dir -r requirements.txt

# Entry point for Vertex AI training
ENTRYPOINT ["python", "train.py"]
