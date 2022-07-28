FROM python:3.10.5

RUN mkdir -p /app
WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

ENV PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python

EXPOSE 9294
CMD ["python", "app.py"]
