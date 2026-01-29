# Use lightweight Python base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install pip dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy only relevant files
COPY all_location.py .
COPY geotargets-2025-04-01.csv .

# Run the script
CMD ["python", "all_location.py"]
