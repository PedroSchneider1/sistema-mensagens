FROM python:3.13.7-alpine3.21

WORKDIR /app

RUN pip install pyzmq protobuf "psycopg[binary]"

CMD ["python", "main.py"]