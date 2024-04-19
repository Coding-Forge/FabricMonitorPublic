# Use Ubuntu LTS as the base image
#FROM ubuntu:20.04
FROM ubuntu:latest

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y tzdata

# Set the timezone to Eastern Time
ENV TZ="America/New_York"

# Update package information and install necessary tools
RUN apt-get update && apt-get install -y software-properties-common gcc

# Add the deadsnakes PPA to get Python 3.10
RUN add-apt-repository -y ppa:deadsnakes/ppa

# Install Python 3.12
RUN apt-get update && apt-get install -y python3.12 python3-distutils python3-pip python3-apt

# Install cron
RUN apt-get update && apt-get -y install cron

COPY requirements.txt .
COPY .env .

RUN echo "5 * * * * root python3 -m /app.monitor" > /etc/cron.d/my_cron_job

# Make the cron job executable
RUN chmod 0644 /etc/cron.d/my_cron_job

# Install the required Python packages
RUN pip3 install -r requirements.txt

# Set the working directory
WORKDIR /app

# Copy your Python code or other files into the container
COPY app /app

WORKDIR ../

# Set up a cron job to run your Python module every day at 3:00 AM


# Start the cron service
CMD ["cron", "-f"]
