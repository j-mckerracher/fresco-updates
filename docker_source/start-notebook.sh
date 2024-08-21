#!/bin/bash

# Generate a config file with the necessary settings
mkdir -p /home/jovyan/.jupyter
echo "c.ServerApp.ip = '0.0.0.0'" >> /home/jovyan/.jupyter/jupyter_server_config.py
echo "c.ServerApp.open_browser = False" >> /home/jovyan/.jupyter/jupyter_server_config.py
echo "c.ServerApp.port = 8888" >> /home/jovyan/.jupyter/jupyter_server_config.py
echo "c.ServerApp.allow_origin = '*'" >> /home/jovyan/.jupyter/jupyter_server_config.py
echo "c.ServerApp.allow_remote_access = True" >> /home/jovyan/.jupyter/jupyter_server_config.py
echo "c.ServerApp.token = ''" >> /home/jovyan/.jupyter/jupyter_server_config.py
echo "c.ServerApp.root_dir = '/home/jovyan'" >> /home/jovyan/.jupyter/jupyter_server_config.py
echo "c.ServerApp.shutdown_no_activity_timeout = 60" >> /home/jovyan/.jupyter/jupyter_server_config.py

# Set the time limit (in seconds) for the notebook session
TIME_LIMIT=$((60*60))  # 1 hour limit

# Start Jupyter Server (Jupyter Lab) with the --allow-root flag
exec jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root &

# Get the process ID of the Jupyter Lab server
JUPYTER_PID=$!

# Sleep for the duration of the time limit
sleep $TIME_LIMIT

# After the time limit, kill the Jupyter server process
kill $JUPYTER_PID