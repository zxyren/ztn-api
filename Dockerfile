FROM python:3.13.7-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    gcc \
    libffi-dev \
    ca-certificates \
    build-essential \
    wget \
    curl \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && node -v

# Upgrade pip and install Python dependencies
RUN pip install --no-cache-dir --upgrade pip

RUN pip install --no-cache-dir \
    flask \
    flask-cors \
    gunicorn \
    pycryptodomex \
    websockets \
    brotli \
    certifi \
    curl-cffi

WORKDIR /app

COPY cookies.txt /cookies.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["gunicorn", "-w", "2", "-k", "gthread", "--threads", "4", "--timeout", "300", "-b", "0.0.0.0:8000", "app:app"]
