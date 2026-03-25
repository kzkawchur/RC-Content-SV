FROM python:3.9-slim-buster

RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y ffmpeg opus-tools python3-pip python3-dev gcc g++ make cmake

WORKDIR /app
COPY . /app/

RUN pip3 install --no-cache-dir -U pip
# এখানে force-reinstall ব্যবহার করা হয়েছে যাতে কোনো পুরোনো ক্যাশ ঝামেলা না করে
RUN pip3 install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1

CMD ["python3", "main.py"]
