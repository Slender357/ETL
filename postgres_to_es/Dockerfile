FROM python:3.10

# Выберите папку, в которой будут размещаться файлы проекта внутри контейнера
WORKDIR /etl

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip  \
    && pip install -r requirements.txt --no-cache-dir \
    && mkdir -p /etl/storage

COPY . .

ENTRYPOINT ["python", "main.py"]