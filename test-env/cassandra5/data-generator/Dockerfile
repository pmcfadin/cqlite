# Use a specific, lightweight version of Python for reproducibility.
FROM python:3.9-slim-bullseye

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file first to leverage Docker's build cache.
# This step only re-runs if requirements.txt changes.
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# The command to run when the container starts.
# We will pass the Cassandra host as an environment variable.
CMD ["python", "populate.py"] 