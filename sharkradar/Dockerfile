# Base image for python applications
FROM python:3.6-alpine

# Installing
RUN python3.6 -m pip install sharkradar

# Create app directory
RUN mkdir sharkradar

# Setting WORKDIR
WORKDIR sharkradar

# Service Port
EXPOSE 16461

CMD ["sharkradar", "--addr", "10.0.0.2", "--dbpath", "."]