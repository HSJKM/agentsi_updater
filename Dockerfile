FROM python:3.10-slim
WORKDIR /code/app/
RUN apt-get update
RUN apt-get install -yy -q bash
COPY ./Requirements.txt /code/requirements.txt
RUN python3 -m pip install --upgrade pip
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
COPY ./app/*.py /code/app/
WORKDIR /code/
#RUN chmod +x start.sh
EXPOSE 80
CMD uvicorn main:app --host 0.0.0.0 --port 80 --workers 2
