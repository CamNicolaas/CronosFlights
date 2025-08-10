<p align="center">
  <img src="https://i.imgur.com/cnc052h.png" alt="Portada" width="650"/><br><br>

  <img src="https://img.shields.io/badge/python-3.12%2B-blue?style=for-the-badge&logo=python" alt="Python version" />
  <img src="https://img.shields.io/badge/Dataset-Actualizado%20diariamente-success?style=for-the-badge" alt="Dataset Actualizado" />
  <img src="https://img.shields.io/github/commit-activity/m/CamNicolaas/CronosFlights?style=for-the-badge" alt="GitHub commit activity" />
</p>


# ✈️ Cronos Flights - Encontrá tus próximas vacaciones
Buscar buenos precios para pasajes en avión suele ser un trabajo e inversión de tiempo grande y muchas veces no termina siendo la opción más barata.
CronosFlights es un proyecto de monitoreo, en tiempo real, de precios de pasajes de avión para Argentina. Su objetivo es simplificar la búsqueda de buenas ofertas para viajeros que están planificando sus próximas vacaciones

### ❓¿Qué hace CronosFlights?
- 🔍Recopila y procesa precios de diferentes aerolíneas y agencias de viaje.
- 🎯Detecta y notifica oportunidades de descuentos en el momento.
- 📈Ofrece datasets actualizados cada 30 minutos, disponibles directamente desde el repositorio.

---

## 📌Sobre el proyecto
Encontrar un buen precio no solo implica invertir tiempo, sino probar diferentes navegadores, limpiando cookies o incluso usar una VPN, técnicas poco cómodas o habituales para el usuario común. CronosFlights automatiza el proceso recopilando y filtrando datos de distintos proveedores para encontrar oportunidades interesantes y facilitar el trabajo de comprar un pasaje acorde.

Y para quienes trabajan en el mundo de los datos, también ofrece la posibilidad de consumir data actualizada, cada 30 minutos se exportan las tablas desde la base de datos y se actualizan directo al repositorio. Es una buena oportunidad para practicar análisis, armar dashboards, diseñar algún ETL o simplemente divertirse analizando cómo varían los precios en Argentina.


## ​✨Características Principales
- **🚀Scraping Automatizado y Asincrónico:** Extrae continuamente los precios publicados en los calendarios de vuelos de aerolíneas y agencias, utilizando procesos async, optimizando el rendimiento y mejorando el tiempo al recuperar los datos.
- **🧩Procesamiento de datos:** Utiliza un pipeline Producer/Consumer, basado en Kafka para delegar responsabilidades entre procesos y evitar bloqueos entre procesos.
- **📊Análisis de ofertas:** Aplica métodos como el rango intercuartílico (IQR) y percentiles para identificar caídas de precio, evitando comparaciones estáticas y brindando flexibilidad.
- **🔔Notificaciones en tiempo real:** Cuando se detectan oportunidades, el sistema arma y envía Embeds a los diferentes canales en Discord, pudiendo el usuario discriminar por origen-destino.
- **⚙️Gestión automática de Tokens:** Para proveedores que requieren autenticación, el sistema incorpora un módulo que detecta, extrae y actualiza tokens de forma automática, garantizando acceso continuo a las diferentes API.
- **🗂️Exportación periódica en formato Parquet:** Cada 30 minutos se genera una exportación de vuelos activos, rutas o secciones y estadísticas, utilizando el formato `.parquet`. Estos archivos se suben automáticamente al repositorio y a una instancia S3 de AWS.


## 🏗️Estructura del proyecto
```
.
├── docker-compose.yml           # Orquestación de servicios: Kafka, PostgreSQL y MongoDB.
├── requirements.txt             # Dependencias del proyecto.
├── example_env.txt              # Plantilla para variables de entorno.
├── FlightsData/                 # Data exportada cada 30 min. (en formato Parquet).
├── modules/                     # Lógica por proveedor de vuelos.
│   └── AerolineasARG/           # Módulo actual para Aerolíneas Argentinas.
│       ├── flightsManager/      # Producer y Consumer Kafka para flujos de vuelos.
│       ├── notifyer/            # Servicio de notificación vía Discord Webhooks.
│       ├── scraper/             # Scraper con lógica específica para Aerolíneas Argentinas.
│       ├── tokensManager/       # Búsqueda, almacenamiento y gestión de tokens.
│       └── tools/               # Análisis de ofertas, actualización de estadísticas y herramientas de aeropuertos.
└── utils/                       # Módulos de utilidad compartidos.
    ├── DB/                      # Conexión y manejo de PostgreSQL y MongoDB.
    ├── configs/                 # Plantilla para config. de los servicios de monitoreo, notificación, kafka, y stats.
    ├── exceptions/              # Excepciones de cada módulo.
    ├── fetchsmethods/           # Módulo encargado de realizar las operaciones fetch.
    ├── kafkamanager/            # Wrapper async para Kafka (producers/consumers).
    ├── logs/                    # Sistema de logs.
    ├── notifys/                 # Módulos de notificación vía Discord Webhooks.
    └── tools/                   # Módulo Singleton y funciones para generar fechas.
```

> [!IMPORTANT]  
> Esta versión del repositorio es una muestra del sistema, con toda su arquitectura, procesamiento y lógica de análisis implementada.  
> Las funciones encargadas de administrar las solicitudes (`utils/fetchsmethods/`) no están incluidas, ya que implican técnicas específicas que no son públicas por motivos de protección y sostenibilidad del proyecto.


## 🚀Proceso de ejecución:
### 1. Prerrequisitos
*   [Docker](https://www.docker.com/get-started)
*   [Python3](https://www.python.org/downloads/)

### 2. Clonar el repositorio
```bash
git clone https://github.com/CamNicolaas/CronosFlights.git
cd CronosFlights
```

### 3. Configurar variables de entorno
Usá el archivo de ejemplo para definir las variables de entorno:
```bash
cp example_env.txt .env
```
Abra `.env` y complete las credenciales requeridas para PostgreSQL, MongoDB, Kafka, AWS y GitHub. Estas son utilizadas tanto por la aplicación como por el archivo `docker-compose.yml`.

### 4. Configuración de servicios.
Revisa el archivo de configuración en `utils/configs/general_configs_example.json`. Deberás crear tu propio `general_configs.json` a partir de este ejemplo.
```bash
cp utils/configs/general_configs_example.json utils/configs/general_configs.json
```

> [!IMPORTANT]
> Antes de ejecutar el sistema de monitoreo de vuelos, es necesario configurar el archivo de settings. Se define el comportamiento de los servicios de scraping, detección de ofertas y notificación. A continuación se detalla la estructura del archivo `general_configs.json`:

```text
{
  "monitor_configs": {
    "admin_configs": {
      // Es obligatorio, se enviaran mensajes notificando de posibles errores.
      "webhooks": {
        "status_monitors": null,
        "filtered": ""
      }
    },
    "flights": {
      "deals_configs": {
        "min_real_price": 1.05,             // Precio mínimo base (relación real)
        "extreme_threshold": 1.5,           // Umbral de precios extremos
        "extreme_percentage_off": 40,       // % de descuento para considerar "extremo"
        "extreme_min_percentage_off": 20,   // Mínimo % para entrar en la categoría
        "deal_q1_discount": 0.85,           // Descuento basado en el primer cuartil
        "no_iqr_discount": 0.85,            // Descuento cuando no hay IQR
        "deal_min_percentage_off": 15       // Descuento mínimo para considerar "oferta"
      },
      "aerolineas_argentinas": {
        "name": "AerolineasARG",
        "link": "https://www.aerolineas.com.ar/",
        "imagen": "https://...",

        // Intervalos de ejecución (en segundos)
        "delay": 10,
        "delay_error": 10,
        "tokens_max_error": 10,

        // Concurrencia del sistema
        "max_threads_consumer": 100,
        "max_threads_monitor_month": 250,
        "max_month_scraping": 12,

        // Aeropuertos que serán monitoreados
        "airports_scraping": {
          // Agregar un "_" en el key para evitar monitorear vuelos desde dicho origen.
          "Buenos Aires": ["AEP", "EZE"],
          "Mar del Plata": ["MDQ"],
          ...
        },

        // Configuración de Topics en Kafka
        "kafka_topics": {
          "producer_to_etl": {
            "name": "flights.aerolineasArg.raw.calendar",
            "group_id": "flights-consumer-group",
            "max_workers": 300,
            "partitions": 3,
            "replication_factor": 3
          },
          "etl_to_notifier": {
            "name": "flights.aerolineasArg.to_notify",
            "group_id": "flights-consumer-group",
            "max_workers": 300,
            "partitions": 3,
            "replication_factor": 3
          }
        },

        // Webhooks de Discord por aeropuerto
        "discord_webhooks": {
          "AEP": null,
          "EZE": null,
          "BHI": null,
          ...
        }
      }
    }
  }
}

```
### 5. Iniciar infraestructura de servicios:
Inicie los Servicios (Kafka, PostgreSQL, MongoDB) usando docker:
```bash
docker-compose up -d
```
El comando extraerá las imágenes necesarias e iniciará los contenedores en segundo plano. El contenedor PostgreSQL también inicializará automáticamente el esquema de la base de datos mediante `utils/DB/flights_db.sql`.

### 6. Instalar Dependencias
Se recomienda utilizar un entorno virtual.
```bash
# Instalar virtualenv (si aún no lo tenes).
pip install virtualenv

# Crear el entorno virtual
virtualenv .

# Activar el entorno
source ./bin/activate

# Instalar las dependencias del proyecto
pip install -r requirements.txt
```

### 7. Ejecución de los módulos
El proyecto está orientado a soportar diferentes proveedores entre aerolíneas y agencias. Actualmente el único proveedor soportado es *Aerolíneas Argentinas*, la estructura actual permite fácilmente integrar nuevos módulos.

Cada proveedor se estructura como un submódulo dentro del directorio `modules/`, con sus propias tareas de scraping, análisis y notificación.

*   **Ejecutar un módulo específico:**
    ```bash
    python modules/<NombreProveedor>/<RutaAlModulo>.py
    ```
*   **Ejemplo: Iniciar flujo del módulo encargado de extraer, transformar y cargar nuevos Tokens:**
    ```bash
    python modules/AerolineasARG/tokensManager/finder_tokens.py 
    ```
> [!NOTE]  
> Cada módulo puede depender de otros componentes para su correcto funcionamiento y cumplir con el propósito, ej: el módulo de `tokensManager` tiene su parte `Finder` y su parte `Updater`.


## 🗂️Acceso a Datos Públicos:
Como parte del proyecto, quise aportar un pequeño dataset interesante para devs que están en el área de data, con el que se puede practicar distintos tipos de análisis: comportamiento de precios, disponibilidad de rutas, cantidad de vuelos por día, entre otros.
También podés utilizar esta data para desarrollar dashboards interactivos, que se actualizan de forma periódica.

### 📚¿Donde esta la data?
Todos los vuelos extraídos y procesados del proyecto, se exportan automáticamente al directorio `FlightsData/`.
Vas a encontrar archivos `.parquet` separados según las diferentes tablas de la base de datos actual.
Podes usar `pandas` para poder leer directamente el contenido de los archivos:

```python
import pandas as pd

# Podes cambiar el nombre del archivo actual "flights_calendar.parquet", por el que necesites.
df = pd.read_parquet(
  "https://raw.githubusercontent.com/CamNicolaas/CronosFlights/main/FlightsData/AerolineasARG/flights_calendar.parquet"
)
print(df.info())
```


## 🌱Planes a futuro
Es la primera versión a modo prueba y demostración de algunas habilidades, pero sueño con mejorar a futuro este proyecto. Algunas ideas actuales son:

<p align="center">
  🔴 Aún no iniciado &nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;&nbsp; 🟡 En progreso &nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;&nbsp; 🟢 Integrado
</p>

- **🔴 | 🛫Nuevos Proveedores:** Actualmente el sistema funciona con *Aerolíneas Argentinas*. A futuro, planeo integrar nuevas aerolíneas y agencias de viaje, con la idea de ampliar la cobertura y comparativa de precios.
- **🟡 | 📅Soporte de rango de fechas:** En esta primera versión solo se analizan vuelos de ida. Un cambio interesante es implementar búsquedas combinadas (ida y vuelta), permitiendo obtener más ofertas y precios más bajos.
- **🟡 | 📊Integración con Dashboard:** Planeo construir un dashboard público con Power BI, donde se visualizan mejores ofertas, estadísticas por ruta, tendencias de precios, top rutas, diferencia entre proveedores, etc. Idealmente estará conectado a AWS S3 - Athena para facilitar su deploy.
- **🔴 | 🐳Modularización y arquitectura:** Una de las metas es contenerizar cada proveedor de forma independiente con Docker. Permitirá escalar, mantener y desplegar más fácilmente los módulos según la aerolínea o agencia.
- **🔴 | 🌎Soporte vuelos al exterior:** Aunque la idea inicial fue vuelos dentro de Argentina, con la intención de viajar dentro de este hermoso país, me gustaría sumar soporte para vuelos internacionales, especialmente hacia los destinos más buscados por argentinos, como Brasil, Chile, Uruguay, Estados Unidos y Europa.

## ⭐ ¿Te gustó el proyecto?
Si llegaste hasta acá y el proyecto te pareció interesante o útil, te invito a dejarle una ⭐ al repo. Es una buena forma de apoyar y motivar a seguir mejorando.
Y si llegás a armar algún dashboard o análisis interesante con los datos del proyecto, me gustaría poder verlo, abrí un issue o escribime por Linkedin.

<p align="center">
  <!-- Follow me on GitHub -->
  <a href="https://github.com/CamNicolaas/" target="_blank">
    <img src="https://img.shields.io/badge/Follow%20me-GitHub-181717?logo=github&logoColor=white&style=for-the-badge" alt="Follow me on GitHub" />
  </a>

  <a href="https://github.com/CamNicolaas/CronosFlights/issues" target="_blank">
    <img src="https://img.shields.io/badge/GitHub%20Issues-Open%20an%20Issue-blue?logo=github&logoColor=white&style=for-the-badge" alt="GitHub Issues" />
  </a>

  <a href="https://www.linkedin.com/in/campos-nicolas/" target="_blank">
    <img src="https://img.shields.io/badge/Connect-LinkedIn-0077B5?logo=linkedin&logoColor=white&style=for-the-badge" alt="LinkedIn" />
  </a>
</p>

Gracias por tomarte el tiempo de ver este proyecto ❤️.
