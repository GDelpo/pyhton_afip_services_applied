# Consumidor de Datos AFIP | ARCA API

Este proyecto es un **cliente** que consume servicios de AFIP mediante la clase `AFIPService`. Su principal función es procesar datos (NITs), consultar el servicio de inscripción (u otros) expuesto en la API de AFIP | ARCA WSN TO API y generar reportes en formato JSON. Además, gestiona errores, reintentos y la autenticación con tokens JWT de forma automática.

---

## Descripción

El cliente realiza las siguientes tareas:

- **Autenticación:**  
  Obtiene un token de acceso (JWT) enviando las credenciales a la API a través del endpoint `/token`. Se renueva el token de forma automática en caso de expiración.

- **Consulta de Servicios:**  
  Mediante el método `fetch_data_service`:
  - Divide la lista de NITs en fragmentos (chunks) según un tamaño predefinido.
  - Realiza llamadas consecutivas al servicio (por ejemplo, `inscription` o `padron`).
  - Implementa reintentos con retroceso exponencial ante fallos o respuestas vacías.

- **Manejo de Errores y Validación:**  
  - Verifica la validez del servicio y del token.
  - Extrae y acumula errores específicos de cada registro, limpiando la respuesta de datos nulos o vacíos.
  - Transforma la respuesta (lista de diccionarios) en un único diccionario indexado por NIT para facilitar el análisis posterior.

- **Control de Llamadas:**  
  Implementa una pausa automática tras un número máximo de llamadas consecutivas, ayudando a respetar las restricciones de rate limiting.

---

## Integración con AFIP | ARCA WSN TO API

Este cliente consume el servicio backend desarrollado en el repositorio [pyhton_fastapi_afip_services](https://github.com/GDelpo/pyhton_fastapi_afip_services).  
Dicha API, implementada con FastAPI, integra los web services de AFIP (WSAA y WSN) e incluye:

- **Autenticación JWT.**
- **Control de Rate Limiting.**
- **Logging personalizado.**
- **Despliegue simplificado con Docker Compose.**

> [!TIP]
> Se recomienda revisar el repositorio mencionado para comprender la exposición del servicio backend y su configuración.

---

## Funcionamiento Interno de `AFIPService` (afip_service.py)

La clase `AFIPService` encapsula la lógica principal del cliente, destacando:

- **Inicialización y Configuración:**  
  Parámetros como:
  - `chunk_size`: Máximo tamaño de fragmento para la lista de NITs.
  - `max_calls` y `pause_duration`: Controlan las llamadas consecutivas y la pausa automática.
  - `max_retries` y `retry_delay`: Estrategia de reintentos con retroceso exponencial.
  - `services_available`: Servicios disponibles (por defecto, `inscription` y `padron`).

- **Autenticación:**  
  Métodos `_get_token()` y `_refresh_token()` gestionan la obtención y renovación del token JWT.

- **Consulta y Reintentos:**  
  - `_query_service()`: Envía la solicitud POST al endpoint adecuado, limpia la respuesta y maneja la renovación del token en caso de error 401.
  - `_request_with_retry()`: Reintenta la consulta hasta alcanzar el máximo de reintentos.

- **Agregación y Formateo:**  
  - `fetch_data_service()`: Combina fragmentación, consulta, reintentos y formateo para obtener un único diccionario de resultados.
  - `format_response()`: Convierte la lista de diccionarios en un diccionario indexado por NIT.

- **Manejo de Errores:**  
  - `extract_errors()` y `accumulate_errors_in_data()`: Identifican, extraen y acumulan errores de cada registro basándose en claves específicas.

---

## Requisitos Previos

- **Python 3.7 o superior**
- Dependencias listadas en `requirements.txt`
- Archivo `.env` configurado con:
  - Credenciales AFIP (`AFIP_USERNAME`, `AFIP_PASSWORD`)
  - URL base de la API (`AFIP_BASE_URL`)
  - Parámetros de fragmentación, reintentos y pausas (`AFIP_CHUNK_SIZE`, `AFIP_MAX_CALLS`, `AFIP_PAUSE_DURATION`, `AFIP_MAX_RETRIES`, `AFIP_RETRY_DELAY`)
  - Lista de servicios disponibles (opcional)
  - Ruta del archivo Excel con los NITs (`EXCEL_FILE_PATH`)
- Un archivo Excel con la columna `nro_nit` (o modificar el script para obtener datos de otra fuente).

---

## Instalación y Configuración

1. **Crea y activa un entorno virtual:**

    ```bash
    python3 -m venv venv
    source venv/bin/activate  # En Windows: venv\Scripts\activate
    ```

2. **Instala las dependencias:**

    ```bash
    pip install -r requirements.txt
    ```

3. **Configura el archivo `.env`** en la raíz del proyecto con las siguientes variables:

    ```env
    AFIP_USERNAME=<tu_usuario>
    AFIP_PASSWORD=<tu_contraseña>
    AFIP_BASE_URL=<url_base_de_la_api>
    AFIP_CHUNK_SIZE=<tamaño_del_chunk>
    AFIP_MAX_CALLS=<número_máximo_de_llamadas_consecutivas>
    AFIP_PAUSE_DURATION=<duración_de_pausa_en_segundos>
    AFIP_MAX_RETRIES=<número_máximo_de_reintentos>
    AFIP_RETRY_DELAY=<retraso_inicial_para_reintentos>
    AFIP_SERVICES_AVAILABLE=<lista_de_servicios_separados_por_coma>
    EXCEL_FILE_PATH=<ruta_al_archivo_excel>
    ```

4. **Verifica** que el archivo Excel con los NITs esté correctamente formateado y ubicado en la ruta especificada.

---

## Uso

> `nit_errors_report.py` es un script de ejemplo que muestra cómo utilizar la clase `AFIPService` para procesar una lista de NITs y generar reportes de errores y registros válidos.

El script realiza las siguientes tareas:

- Lee el archivo Excel con la lista de NITs.
- Consulta el servicio de inscripción en AFIP a través de la clase `AFIPService`.
- Acumula y filtra los errores detectados.
- Genera dos reportes en formato JSON:
  - Uno con los errores encontrados.
  - Otro con los registros procesados correctamente.
- Genera logs en el archivo `error_report.log`.

Para ejecutar el cliente:

```bash
python nit_errors_report.py
```

---

> `monotributo_checker.py` amplía la funcionalidad del cliente al procesar información adicional relacionada con el monotributo y el régimen general de las personas consultadas. Entre sus principales funciones se encuentran:

- **Carga y Configuración:**
  - Carga las variables de entorno definidas en el archivo `.env`.
  - Lee el archivo Excel que contiene los NITs, asegurándose de que la columna `nro_nit` esté correctamente formateada.

- **Consulta y Procesamiento de Datos:**
  - Inicializa una instancia de `AFIPService` utilizando los parámetros configurados.
  - Consulta el servicio de inscripción de la API AFIP.
  - Acumula y filtra los errores detectados en la respuesta.
  - Procesa cada registro para extraer y formatear datos relevantes, como:
    - Datos generales (tipo de persona, nombre, apellido o razón social).
    - Información de monotributo y/o régimen general.
  - Solo se incluyen aquellos registros que tengan datos de monotributo o de régimen general.

- **Generación de Reporte:**
  - Guarda un reporte en formato JSON que contiene los registros con información relevante.
  - Registra logs detallados en el archivo `monotributo.log`.

### Ejecución del Script

Para ejecutar el script de monotributo, utiliza el siguiente comando en la terminal:

```bash
python monotributo_checker.py
```

> [!TIP]
> Asegúrate de que el archivo Excel y las variables de entorno estén correctamente configurados para evitar errores durante la ejecución.

---

## Contribución

Si deseas contribuir, realiza un fork del repositorio, implementa tus mejoras y envía un pull request. Toda contribución que mejore la gestión de errores, la optimización de consultas o la ampliación de funcionalidades es bienvenida.

---