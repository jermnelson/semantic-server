# Semantic Server REST API Dockerfile
FROM python:3.4.3
MAINTAINER "Jeremy Nelson <jermnelson@gmail.com>"

# Install dependancies 
RUN apt-get install git \
    && git clone https://github.com/jermnelson/semantic-server.git /opt/semantic-server \
    && cd /opt/semantic-server/ \    
    && git checkout -b development \
    && git pull origin development \
    && pip3 install -r requirements.txt

WORKDIR /opt/semantic-server/

# Runs on port 18150 (in honor of Ada Lovelace's 200th birthday)
EXPOSE 18150

CMD python app.py
