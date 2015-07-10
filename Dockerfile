# Semantic Server REST API Dockerfile
FROM python:3.4.3
MAINTAINER "Jeremy Nelson <jermnelson@gmail.com>"

RUN pip3 install falcon \
    && pip3 install rdflib \
    && pip3 install Werkzeug

# Run's on port 18150 (in honor of Ada Lovelace's 200th birthday)
EXPOSE 18150
