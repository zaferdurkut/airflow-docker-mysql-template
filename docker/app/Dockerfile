FROM ubuntu:16.04

MAINTAINER Zafer Durkut, <mailto:durkutzafer@gmail.com>

RUN apt-get update \
   && apt-get upgrade -y \
   && apt-get install -y \
   build-essential \
   ca-certificates \
   gcc \
   libpq-dev \
   libmysqlclient-dev \
   make \
   python3-dev \
   python3-pip \
   ssh \
   supervisor \
   && apt-get autoremove \
   && apt-get clean

WORKDIR /app
COPY . /app
COPY supervisord.conf /etc/supervisor/conf.d/airflow.conf


RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt
RUN pip3 install cryptography --force-reinstall
RUN airflow upgradedb

# servis otomatik baslatilmayacak ise asagidaki komut yorum satirina alinmalidir.
# CMD ["/usr/bin/supervisord"] 
