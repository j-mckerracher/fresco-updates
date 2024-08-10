#!/bin/bash

# Generate a config file with the necessary settings
echo "c.NotebookApp.ip = '0.0.0.0'" >> /home/work/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.open_browser = False" >> /home/work/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.port = 8888" >> /home/work/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.allow_origin = '*'" >> /home/work/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.allow_remote_access = True" >> /home/work/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.token = ''" >> /home/work/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.notebook_dir = '/home/work'" >> /home/work/.jupyter/jupyter_notebook_config.py

# Start Jupyter Notebook
exec jupyter notebook "$@"
