FROM python:3.6-stretch

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ob_gen_kafka.py ./

CMD [ "python", "./ob_gen_kafka.py" ]