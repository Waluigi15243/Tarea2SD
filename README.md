# Tarea2SD
1. Para que el código funcione correctamente, es necesario instalar las dependencias en el archivo requirements.txt mediante el comando "pip install -r requirements.txt", además de utilizar el comando "pip install flask" para permitir el recibo de peticiones HTTP en el código

2. Para levantar el contenedor, se utiliza "docker compose up -d"

3. Para enviar peticiones HTTP POST al codigo peticiones.py, utilice el comando "curl -X POST -d value=[algún número] localhost:3000/"

4. Para enviar peticiones HTTP GET para saber el estado de una transacción en notificaciones.py, utilice el comando "curl -X GET localhost:5000/?value=[algún número]"
