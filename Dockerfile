FROM python:3.12

ENV PYTHONUNBUFFERED 1

WORKDIR /app

EXPOSE 8000

COPY . /app

RUN pip install -r requirements.txt

CMD ["python", "src/main.py"]