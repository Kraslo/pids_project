#!/bin/bash

# Iniciar el servicio SSH (para habilitar SFTP)
service ssh start

# Ejecutar el comando estándar de Airflow (sin afectar lo que ya hace Airflow por defecto)
exec /usr/local/bin/docker-entrypoint.sh "$@"
