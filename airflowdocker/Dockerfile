# Usamos la imagen base de Airflow
FROM apache/airflow:2.10.2

# Cambiar a root para poder instalar paquetes del sistema
USER root

# Instalar OpenSSH para habilitar el acceso SFTP
RUN apt-get update && apt-get install -y \
    openssh-server \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Crear un usuario sftpuser con su carpeta de destino para los archivos
RUN useradd -m sftpuser && \
    echo "sftpuser:sftppassword" | chpasswd && \
    mkdir -p /home/sftpuser/uploads && \
    chown sftpuser:sftpuser /home/sftpuser/uploads

# Copiar la configuración de sshd para permitir SFTP
COPY sshd_config /etc/ssh/sshd_config
COPY requirements.txt .

USER airflow

RUN pip install -r requirements.txt
# Exponer los puertos necesarios para SSH (SFTP) y la interfaz web de Airflow
EXPOSE 22 8080

# Volver al usuario de Airflow para que el contenedor ejecute Airflow
# USER airflow

# El contenedor sigue ejecutando el flujo de trabajo habitual de Airflow sin cambios
# ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
