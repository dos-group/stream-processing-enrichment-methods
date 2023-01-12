FROM python:3.7
MAINTAINER XenonStack
 
# Creating Application Source Code Directory
RUN mkdir -p /app/src

# Setting Home Directory for containers
WORKDIR /app/src

# Installing python dependencies
COPY requirements.txt /app/src
RUN pip3 install --no-cache-dir -r requirements.txt

# Copying src code to Container
COPY . /app/src/

# Application Environment variables
ENV APP_ENV development

# Exposing Ports
EXPOSE 4025

ENTRYPOINT ["python3", "main.py"]
