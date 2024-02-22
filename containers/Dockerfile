# Use the official Ubuntu base image
FROM ubuntu:latest

# Update package information and install necessary tools
RUN add-apt-repository -y ppa:deadsnakes/ppa

# Install Python 3.10
RUN apt-get update && apt-get install -y python3.10 python3-distutils python3-pip python3-apt

# Install the required Python packages

COPY ./config.json .
COPY ./requirements.txt .

# Create and activate virtual environment
RUN python3 -m venv venv
RUN /bin/bash -c "source venv/bin/activate"

# Install the required Python packages
RUN pip3 install -r requirements.txt

# Set the working directory
WORKDIR /app

# Copy your Python code or other files into the container
COPY ./app /app

# Specify the default command to run when the container starts
#CMD ["python3", "-u", "your_script.py"]
CMD ["python3", "-m", "app.monitor"]
