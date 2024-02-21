# Use the latest Ubuntu LTS image as the base
FROM ubuntu:latest

# Set the working directory inside the container
WORKDIR /app

# Copy the app folder from the host into the container
COPY app /app
