FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

EXPOSE 8080
CMD streamlit run /app/app.py --server.address=0.0.0.0 --server.port=8080
