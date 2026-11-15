FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Keep the package layout: /srv/app/… so `app.app:app` resolves as a package
# (app.py uses relative imports and cannot run flattened into /srv).
COPY . /srv/app

EXPOSE 8000

CMD ["uvicorn", "app.app:app", "--host", "0.0.0.0", "--port", "8000"]
