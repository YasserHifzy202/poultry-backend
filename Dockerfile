FROM python:3.11-slim
WORKDIR /app
COPY ./main.py /app/main.py
COPY requirements.txt /app/requirements.txt

# ثبّت كل المتطلبات من requirements.txt بدل ما تكتبها يدويًا
RUN pip install --no-cache-dir -r requirements.txt

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10000"]
