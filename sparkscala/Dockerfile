FROM apache/spark:3.3.3

# Cambiar al usuario root para instalar SSH y SFTP
USER root
WORKDIR /app

# Copiar archivos de configuración y scripts
COPY script1.py . 
COPY ./start-spark.sh /usr/local/bin/start-spark.sh
COPY requirements.txt .

# Instalar dependencias necesarias: OpenSSH para SFTP y otras herramientas
RUN apt-get update && apt-get install -y \
    openssh-server \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Crear un usuario para SFTP y configurar el directorio de subida
RUN useradd -m sftpuser && \
    echo "sftpuser:sftppassword" | chpasswd && \
    mkdir -p /home/sftpuser/uploads && \
    # Configurar permisos: el directorio Chroot debe ser propiedad de root
    chown root:root /home/sftpuser && \
    chmod 755 /home/sftpuser && \
    # El directorio uploads puede ser propiedad de sftpuser
    chown sftpuser:sftpuser /home/sftpuser/uploads

# Configurar SSH (permitir SFTP)
RUN mkdir /var/run/sshd
COPY sshd_config /etc/ssh/sshd_config

# Exponer puertos necesarios
EXPOSE 22 7077 4040

# Copiar los archivos necesarios para el arranque de Spark
RUN pip3 install -r requirements.txt
RUN chmod +x /usr/local/bin/start-spark.sh

# Cambiar el ENTRYPOINT para arrancar SSH y Spark
ENTRYPOINT ["/usr/local/bin/start-spark.sh"]
