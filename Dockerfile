# Usamos una imagen base de Python 3.9 slim
FROM python:3.9-slim

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Instalar dependencias del sistema necesarias para OpenCV y HTTPS (ca-certificates)
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias Python, incluyendo boto3, OpenCV y otras librerías
RUN pip install --no-cache-dir boto3 opencv-python pillow colorama

# Copiar el script Python dentro del contenedor
COPY Script.py /app/

# Definir comando por defecto al arrancar el contenedor
ENTRYPOINT ["python3", "Script.py"]

