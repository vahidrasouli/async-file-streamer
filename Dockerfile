# ==========================================
# مرحله اول: Builder (ساخت و نصب وابستگی‌ها)
# ==========================================
FROM python:3.11-slim as builder

# جلوگیری از نوشتن فایل‌های کش پایتون و نمایش زنده لاگ‌ها
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /build

# کپی کردن فایل نیازمندی‌ها (requirements.txt باید شامل rubpy و aiohttp باشد)
COPY requirements.txt .

# ساخت یک محیط مجازی (Virtual Environment) و نصب کتابخانه‌ها
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# ==========================================
# مرحله دوم: Runner (اجرای امن و سبک)
# ==========================================
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# ایجاد یک کاربر غیرمجاز (non-root) برای امنیت سرور ابری
RUN useradd -m -r botuser

WORKDIR /app

# کپی کردن محیط مجازی از مرحله Builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# کپی کردن کدهای برنامه
COPY integrated_bot_v1.1_main.py .

# تغییر مالکیت فایل‌ها به کاربر ایمن
RUN chown -R botuser:botuser /app

# سوئیچ کردن روی کاربر ایمن
USER botuser

# دستور اجرای برنامه
CMD ["python", "integrated_bot_v1.1_main.py"]