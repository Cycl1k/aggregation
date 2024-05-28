FROM python:3.12

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

WORKDIR /app 

ADD /app/requirements.txt /app/

ENV MONGO_STR_SRV=''
ENV MONGO_STR_DB=''
ENV MONGO_STR_COL=''
ENV BOT_TOKEN=''

COPY /app /app/

RUN apt-get update \
    && apt-get update -y \
    && pip install -r /app/requirements.txt

CMD [ "python3", "telegram.py"]