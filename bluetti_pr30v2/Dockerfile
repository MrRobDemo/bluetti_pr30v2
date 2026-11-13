# Debian-based образ: для всех зависимостей есть готовые wheel-пакеты,
# компилировать ничего не нужно, сборка занимает ~1-2 минуты.
FROM python:3.12-slim-bookworm

# Версию библиотеки можно зафиксировать: --build-arg BLUETTI_LIB="bluetti-bt-lib==X.Y.Z"
ARG BLUETTI_LIB="bluetti-bt-lib"

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1

# BlueZ уже работает на хосте HAOS; bleak обращается к нему по D-Bus
# (host_dbus: true в config.yaml), отдельные пакеты bluez/dbus не нужны.
RUN pip install --prefer-binary --only-binary=:all: "${BLUETTI_LIB}" "paho-mqtt>=2.0"

COPY app/main.py /app/main.py

CMD ["python3", "-u", "/app/main.py"]
