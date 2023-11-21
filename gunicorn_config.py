# gunicorn_config.py

workers = 1
bind = "0.0.0.0:54323"
timeout = 14400

# Specify the path to your log files
errorlog = "gunicornerror.log"
accesslog = "gunicornaccess.log"
