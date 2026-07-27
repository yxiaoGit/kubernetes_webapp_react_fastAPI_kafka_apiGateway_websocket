FROM python:3.11-alpine
WORKDIR /app
# Installs the modern, active pure-python fork without compilation wheels
RUN pip install kafka-python-ng
COPY worker.py .
CMD ["python", "-u", "worker.py"]

