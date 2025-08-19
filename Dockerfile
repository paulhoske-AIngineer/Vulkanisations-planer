FROM python:3.11-slim

# Nginx installieren
RUN apt-get update && apt-get install -y nginx && rm -rf /var/lib/apt/lists/*

# Streamlit & App
WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py /app/app.py
COPY .streamlit /root/.streamlit
RUN rm -f /etc/nginx/sites-enabled/default
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 8080
CMD streamlit run /app/app.py --server.address=0.0.0.0 --server.port=8501 & \
    nginx -g 'daemon off;'
