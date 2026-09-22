FROM python:3.11-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY requirements.txt constraints.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt -c /app/constraints.txt
COPY . /app
CMD ["python", "-m", "src.cli", "validate-env"]
