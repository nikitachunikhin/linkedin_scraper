# Use an official Python runtime as a parent image
FROM python:3.10.6-slim

WORKDIR /linkedin_scraper

# Install system dependencies and tools
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    wget \
    gnupg \
    libnss3 \
    libgconf-2-4 \
    libxss1 \
    libappindicator1 \
    fonts-liberation \
    libasound2 \
    xdg-utils \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file and install Python dependencies
COPY requirements.txt .
RUN pip3 install --upgrade pip && pip3 install --no-cache-dir -r requirements.txt

# Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws/

# Copy the current directory contents into the container
COPY . .

# Set up environment variables for Chromium
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

# Run bing_search.py when the container launches
CMD ["python3", "bing_search.py"]
