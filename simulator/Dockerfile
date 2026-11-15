FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    SIM_TCP_PORT=5000 \
    SIM_HTTP_PORT=8001

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . /srv

EXPOSE 8001 5000

CMD ["uvicorn", "simulator:app", "--host", "0.0.0.0", "--port", "8001"]
