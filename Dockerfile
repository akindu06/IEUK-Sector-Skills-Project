FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the analyzer code
COPY main.py ./

# Default command: run the analyzer with the provided logfile
ENTRYPOINT ["python", "./main.py"]
