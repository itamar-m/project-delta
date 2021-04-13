# Base image for python applications
FROM python:3.6

# Installing
# RUN ap add gcc python3-dev libssl1.0.0
RUN python3.6 -m pip install requests psutil
RUN python -m pip install ibm_db

# Create app directory
RUN mkdir /usr/src/app
RUN mkdir /usr/src/app/util

# Copying the code to the folder
COPY ./dbGet/dbGet.py /usr/src/app/
COPY ../util/DB2ConnectionHandler.py /usr/src/app/util

# Setting WORKDIR
WORKDIR /usr/src/app

# Service Port
EXPOSE 32001

CMD ["python", "dbGet.py"]