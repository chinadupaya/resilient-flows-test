FROM python:3.10-slim
WORKDIR /app
# RUN pip install kafka-python prometheus-client

RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org prometheus-client
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org kafka
COPY detector.py .
CMD ["python", "detector.py"]