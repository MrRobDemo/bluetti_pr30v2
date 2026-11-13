ARG BUILD_FROM=ghcr.io/home-assistant/base:latest
FROM ${BUILD_FROM}

# Версию библиотеки можно зафиксировать: --build-arg BLUETTI_LIB="bluetti-bt-lib==X.Y.Z"
ARG BLUETTI_LIB="bluetti-bt-lib"

RUN apk add --no-cache python3 py3-pip dbus bluez \
    && apk add --no-cache --virtual .build-deps gcc musl-dev python3-dev libffi-dev \
    && pip3 install --no-cache-dir --break-system-packages "${BLUETTI_LIB}" "paho-mqtt>=2.0" \
    && apk del .build-deps

COPY app/main.py /app/main.py

CMD ["python3", "-u", "/app/main.py"]
