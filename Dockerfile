# Base image for python applications
FROM python:3.6-alpine

# Installing
RUN python3.6 -m pip install requests

# Create app directory
RUN mkdir /usr/src/app

# Copying the code to the folder
COPY apiGateway.py /usr/src/app/

# Setting WORKDIR
WORKDIR /usr/src/app

# Service Port
EXPOSE 8080

CMD ["python", "apiGateway.py"]