FROM python:3.11-slim

WORKDIR /app
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org kafka-python
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org google-cloud-spanner

COPY poller.py .

CMD ["python", "poller.py"]

# kafka-python
# google-cloud-spanner