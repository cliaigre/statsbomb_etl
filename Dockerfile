FROM python:3.11-slim

ARG ETL_MODE

RUN apt-get update && \
	apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY . . 

RUN pip3 install --no-cache-dir --upgrade pip 
RUN pip3 install --no-cache-dir -r requirements.txt

WORKDIR /script

CMD ["sh", "-c", "python3 etl.py ${ETL_MODE}"]