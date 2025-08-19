FROM python:3.11-slim

RUN apt-get update && apt-get install -y nginx && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 1) Abhängigkeiten zuerst (Cache bleibt wirksam)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2) Jetzt das **gesamte Repo** in /app kopieren (damit ist app.py sicher dabei)
COPY . /app

# 3) Nginx so konfigurieren wie gebraucht
RUN rm -f /etc/nginx/sites-enabled/default
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 8080
CMD streamlit run /app/app.py --server.address=0.0.0.0 --server.port=8501 & \
    nginx -g 'daemon off;'
