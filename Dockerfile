FROM python:3.11-slim-bookworm
ARG BUILD_REVISION=unknown
ARG TORCH_INDEX_URL=https://download.pytorch.org/whl/cpu
LABEL org.opencontainers.image.revision=$BUILD_REVISION
ENV BUILD_REVISION=$BUILD_REVISION
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 YOLO_CONFIG_DIR=/tmp/ultralytics OPENCV_LOG_LEVEL=OFF
RUN apt-get update && apt-get install -y --no-install-recommends libgl1 libglib2.0-0 ffmpeg && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir --index-url "$TORCH_INDEX_URL" torch==2.6.0 torchvision==0.21.0 && pip install --no-cache-dir -r requirements.txt
RUN groupadd -g 10001 poolsafe && useradd -u 10001 -g 10001 -M poolsafe && mkdir -p /dados /modelos /entrada && chown 10001:10001 /dados
COPY *.py ./
USER 10001:10001
CMD ["python", "main.py"]
