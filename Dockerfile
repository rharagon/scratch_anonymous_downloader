# Usa una imagen base
FROM python:3.10

LABEL maintainer="daniesmor"

# Set Python environment variables

# Instalar dependencias del sistema
RUN rm -rf /var/lib/apt/lists/*

# Establece el directorio de trabajo
WORKDIR /var/www

# Añadir la aplicación al contenedor
ADD . /var/www/

# Actualizar pip e instalar dependencias de Python
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Exponer el puerto de la aplicación
EXPOSE 8000
