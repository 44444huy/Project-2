FROM python:3.12-slim

WORKDIR /opt/app

COPY app/requirements.txt /opt/app/requirements.txt
RUN pip install --no-cache-dir -r /opt/app/requirements.txt

COPY app/dashboard.py /opt/app/dashboard.py

EXPOSE 8501

CMD ["streamlit", "run", "/opt/app/dashboard.py", "--server.address=0.0.0.0", "--server.port=8501"]
