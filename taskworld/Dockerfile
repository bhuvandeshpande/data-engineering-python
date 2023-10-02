FROM python:3.9-slim

WORKDIR /usr/src/app

COPY . .

COPY wait-for-postgres.sh ./
RUN chmod +x wait-for-postgres.sh

RUN pip install --trusted-host pypi.python.org -r requirements.txt
RUN apt-get update && apt-get install -y postgresql-client

CMD ["./wait-for-postgres.sh", "postgres", "python", "./main.py"]
