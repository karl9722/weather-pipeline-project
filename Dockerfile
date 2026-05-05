FROM python:3.11-slim

WORKDIR /app/src/projet_final_4DATA

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]