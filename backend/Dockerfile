FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY backend/requirements.txt backend/requirements.txt
# P4 explicitly uses CPU inference. Install the matching CPU build before resolving
# the remaining requirements, avoiding several GB of unused CUDA packages on Linux.
RUN pip install --no-cache-dir --timeout 120 --index-url https://download.pytorch.org/whl/cpu torch==2.15.0 \
    && pip install --no-cache-dir --timeout 120 -r backend/requirements.txt
COPY backend backend
COPY frontend frontend
COPY results results

CMD ["python", "-m", "flask", "--app", "backend.app", "run", "--host", "0.0.0.0", "--port", "5050"]
