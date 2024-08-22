# Use the official Jupyter minimal notebook image as the base image
FROM jupyter/minimal-notebook

# Set AWS credentials and region
ENV AWS_ACCESS_KEY_ID=""
ENV AWS_SECRET_ACCESS_KEY=""
ENV AWS_DEFAULT_REGION=us-east-1

# Install necessary system packages
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential libpq-dev gcc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install necessary Python packages, including boto3 and pyathena
RUN pip install --upgrade pip
RUN pip install matplotlib pandas ipywidgets IPython psycopg2-binary scipy seaborn tqdm boto3 PyAthena pyarrow

# Copy your notebooks, code, and Jupyter config to the container
COPY docker_source /home/jovyan

# Make the start-notebook.sh script executable
RUN chmod +x /home/jovyan/start-notebook.sh

# Set the working directory
WORKDIR /home/jovyan

# Set the default command to start Jupyter Notebook
ENTRYPOINT ["/home/jovyan/start-notebook.sh"]