# Use a modern and secure Python base image
FROM python:3.9-slim
# Set the working directory
WORKDIR /app

# Copy project files
COPY . .

# Install pdm and project dependencies
# We use --no-cache-dir to reduce image size
RUN pip install --no-cache-dir pdm
RUN pdm install --prod --no-editable

RUN pip install pendulum
RUN pip install networkx
RUN pip install apache-airflow

# Set an entrypoint that allows running scripts or an interactive shell
ENTRYPOINT ["pdm", "run"]

