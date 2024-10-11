
FROM python:3.11-slim

WORKDIR /flask_app

COPY . /flask_app

RUN pip install --no-cache-dir psycopg2-binary
RUN pip install --no-cache-dir pandas
RUN pip install --no-cache-dir pyvis
RUN pip install --no-cache-dir flask
RUN pip install --no-cache-dir SQLAlchemy

EXPOSE 5000

ENV FLASK_APP=app.py

CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]
