FROM python:3.9.5
LABEL maintainer="Ramesh"
COPY /data/. /data/
COPY /solution/. /solution/
RUN pip install -r /solution/requirements.txt
WORKDIR /solution
CMD ["python","/solution/src/main.py"]
