# Base image with Python 3.11
FROM python:3.11

# Set working directory inside the container
WORKDIR /app

# Copy requirement file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline script
COPY pipeline.py .

# Set the default command to run the pipeline
CMD ["python", "pipeline.py"]
