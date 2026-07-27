FROM python:3.11-slim
WORKDIR /app
# THE FIX: Added 'websockets' to the dependency list
RUN pip install fastapi uvicorn websockets psycopg2-binary pydantic aiokafka
COPY main.py .
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

