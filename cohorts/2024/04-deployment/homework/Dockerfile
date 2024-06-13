FROM agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim

RUN pip install pipenv

WORKDIR /app

COPY Pipfile* .

RUN pipenv install --deploy --ignore-pipfile

COPY predict.py .

ENTRYPOINT ["pipenv", "run", "python", "predict.py"]