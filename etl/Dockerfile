FROM python:3.11.6
WORKDIR /opt/app
COPY . .
RUN pip install -r requirements.txt
CMD ["python", "load_data.py"]