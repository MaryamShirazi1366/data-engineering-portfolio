# Use Python 3.9 as the base image
FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

# Copy all local files to the container
COPY . /app

# Install dependencies (if you have a requirements.txt file)
RUN pip install --no-cache-dir -r requirements.txt

# Run the main.py script when the container starts
CMD ["python", "jobs/main.py"]
