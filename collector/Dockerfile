FROM python:3.11-slim

# không ghi .pyc, log in ra ngay (xem được bằng docker logs)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# cài thư viện trước để Docker cache lại lớp này khi chỉ sửa code
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app ./app

EXPOSE 8000

# 1 worker vì bộ nhớ chống trùng event_id nằm trong tiến trình (xem app/dedup.py)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
