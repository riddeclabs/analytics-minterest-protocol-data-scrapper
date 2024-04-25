FROM python:3.11.7-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY src .

ENTRYPOINT ["python", "main.py"]
