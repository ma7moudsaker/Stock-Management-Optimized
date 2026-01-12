FROM python:3.9-slim

# تثبيت المكتبات المطلوبة
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# إعداد المجلد
WORKDIR /app

# نسخ وتثبيت المتطلبات
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# نسخ الملفات
COPY . .

# إنشاء المجلدات المطلوبة
RUN mkdir -p static/uploads/products

# إعداد متغيرات البيئة
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# تشغيل التطبيق
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 app:app
