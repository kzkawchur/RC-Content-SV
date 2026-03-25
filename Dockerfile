FROM python:3.9-slim-buster

# প্রয়োজনীয় বিল্ড টুলস এবং FFmpeg ইনস্টল করা
RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y ffmpeg opus-tools python3-pip python3-dev gcc g++ make

WORKDIR /app
COPY . /app/

# লাইব্রেরি ইনস্টল করা
RUN pip3 install --no-cache-dir -U pip
RUN pip3 install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1

CMD ["python3", "main.py"]
