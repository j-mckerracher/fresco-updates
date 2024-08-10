# Use the official Jupyter minimal notebook image as the base image
FROM jupyter/minimal-notebook

# Set environment variables for the notebook
ENV DBHOST=""
ENV DBUSER=""
ENV DBPW=""
ENV DBNAME=""

# Install necessary system packages
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential libpq-dev gcc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install necessary Python packages
RUN pip install --upgrade pip
RUN pip install matplotlib pandas ipywidgets IPython psycopg2-binary scipy seaborn tqdm

# Copy your notebooks and code to the container
COPY docker_source /home/work

# Set the working directory
WORKDIR /home/work

# Set the default command to start Jupyter Lab
CMD ["start-notebook.sh", "--NotebookApp.token=''"]
