FROM apache/airflow:2.9.3-python3.12

USER root

# Cài đặt các thư viện hệ thống cần thiết cho Postgres và Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Sao chép file requirements.txt vào container
COPY requirements.txt .

# Cài đặt các thư viện Python
RUN pip install --no-cache-dir -r requirements.txt

USER root
# Cài đặt các thư viện hệ thống cho Playwright (cần quyền root)
RUN playwright install-deps chromium

USER airflow
# Cài đặt browser binaries (chromium)
RUN playwright install chromium

ENV PYTHONPATH="/opt/airflow"
