FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PORT=10000
EXPOSE 10000
CMD ["sh","-c","gunicorn --bind 0.0.0.0:${PORT} --workers 1 --threads 4 --timeout 120 server:app"]
