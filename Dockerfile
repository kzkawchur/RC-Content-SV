FROM python:3.9-slim-buster

RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y ffmpeg opus-tools python3-pip

WORKDIR /app
COPY . /app/

RUN pip3 install --no-cache-dir -U pip
RUN pip3 install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1

CMD ["python3", "main.py"]
