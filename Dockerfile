FROM python:3.9.0-slim

WORKDIR /app

COPY . /app

# Install any necessary dependencies
RUN apt-get update && \
    apt-get install -y git

# Install any necessary Python packages
RUN pip install -r requirements.txt
