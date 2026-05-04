FROM apache/airflow:2.9.3-python3.12

USER root

# Cài đặt các thư viện hệ thống cần thiết cho Postgres, Playwright và Xvfb
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    # Xvfb: màn hình ảo để chạy trình duyệt "có giao diện" mà không cần headless
    xvfb \
    # Fonts hệ thống: Cloudflare fingerprint kiểm tra fonts
    fonts-liberation \
    fonts-noto-cjk \
    fonts-ipafont-gothic \
    fonts-wqy-zenhei \
    fonts-thai-tlwg \
    fonts-kacst \
    fonts-freefont-ttf \
    fonts-noto-color-emoji \
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
