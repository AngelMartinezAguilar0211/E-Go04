# E-Go04 — Sistema Resiliente de Streaming con Backpressure y Circuit Breaker

Pipeline de procesamiento de eventos en streaming de alta resiliencia desarrollado en **Go** para el escenario crítico de pico de tráfico del **Hot Sale LATAM**. El sistema extiende la arquitectura de streaming basada en **Redis Streams**, incorporando protección mediante **Circuit Breaker** de 3 estados frente a servicios externos degradados, control adaptativo de **Backpressure con Buffer de Espera Bounded**, y pacing de producción con ventana de **Spike de 10x tráfico** durante más de 2 minutos continuos, garantizando estricta **cero pérdida de datos (zero data loss)**.

---

## 1. Arquitectura del Sistema

```
                  ┌────────────────────────────────────────────────────────┐
                  │                 cmd/producer (Paced)                   │
                  │  Ritmo Normal (50 ev/s) -> Spike 10x (500 ev/s, >=2m)  │
                  └──────────────────────────┬─────────────────────────────┘
                                             │ XADD (Pipelines)
                                             ▼
                                  ┌────────────────────┐
                                  │   Redis Streams    │
                                  │  (hotsale:events)  │
                                  └──────────┬─────────┘
                                             │ XREADGROUP
                                             ▼
                  ┌────────────────────────────────────────────────────────┐
                  │                 cmd/consumer (Resilient)               │
                  │                                                        │
                  │  ┌────────────────────┐      ┌──────────────────────┐  │
                  │  │   internal/dedup   │      │ internal/backpressure│  │
                  │  │ (Sliding LRU 250k) │      │  (Retry Buffer FIFO  │  │
                  │  └─────────┬──────────┘      │   + Watermark Ctrl)  │  │
                  │            │                 └──────────▲───────────┘  │
                  │            ▼                            │ Reintentos   │
                  │  ┌────────────────────┐                 │ diferidos    │
                  │  │  internal/breaker  │─────────────────┘              │
                  │  │  (CLOSED/OPEN/HALF)│                                │
                  │  └─────────┬──────────┘                                │
                  │            │ HTTP POST                                 │
                  └────────────┼───────────────────────────────────────────┘
                               ▼
                  ┌────────────────────────┐
                  │  inventory_service.py  │
                  │ (HTTP :8090/check-inv) │
                  └────────────────────────┘
```

### Componentes Principales

1. **`cmd/producer/`**: Productor con rate-limiting token bucket. Publica a un ritmo sostenido normal, transiciona automáticamente a una ventana de spike de 10x durante al menos 2 minutos, y emite el centinela `EOF_SIGNAL`.
2. **`cmd/consumer/`**: Consumidor resiliente multi-goroutine con pool de workers acotado. Deduplica eventos, verifica inventario contra el servicio simulado, gestiona retenciones y confirmaciones (`XACK`), y aplica backpressure modulando la lectura de Redis.
3. **`internal/breaker/`**: Máquina de estados finitos del Circuit Breaker (`CLOSED`, `OPEN`, `HALF_OPEN`). Abre tras fallos consecutivos, respeta el tiempo de cooldown, admite llamadas de prueba en half-open y protege contra el colapso en cascada.
4. **`internal/backpressure/`**: Controlador de contrapresión con umbrales de nivel de agua (*High/Low Watermarks*) y cola FIFO de reintentos con backoff exponencial y cálculo de profundidad máxima alcanzada.
5. **`internal/inventory/`**: Cliente HTTP de alto rendimiento con pooling de conexiones y envoltura transparente del Circuit Breaker.
6. **`internal/dedup/`**: Motor de deduplicación en memoria con capacidad acotada (LRU de 250,000 slots) y complejidad $O(1)$.
7. **`internal/domain/`**: Modelos de eventos y acumulador de agregados de negocio por país thread-safe.

---

## 2. Requisitos Previos

- **Go**: Versión 1.23 o superior.
- **Docker & Docker Compose**: Para levantar el broker Redis Streams en un entorno aislado.
- **Python**: Versión 3.9 o superior (para ejecutar `inventory_service.py`).
- **Dataset**: Archivo `eventos_hotsale.jsonl` en la raíz del repositorio.

---

## 3. Guía de Ejecución Rápida

Todos los comandos deben ejecutarse desde la raíz de este repositorio:

### Paso 1: Iniciar el Broker Redis

```bash
docker compose up -d
```

Verificar que el contenedor `gotich_redis_e_go04` esté saludable:
```bash
docker compose ps
```

### Paso 2: Iniciar el Servicio Simulado de Inventario

En una terminal independiente, ejecutar el servicio de inventario en el puerto `8090`:
```bash
python3 inventory_service.py
```

Verificar su estado inicial:
```bash
curl -s http://localhost:8090/stats
```
Respuesta esperada: `{"current_rps": 0, "total_requests": 0, "total_failures": 0}`.

### Paso 3: Compilar los Binarios

```bash
mkdir -p bin
go build -o bin/consumer ./cmd/consumer
go build -o bin/producer ./cmd/producer
```

### Paso 4: Ejecutar la Suite de Pruebas Unitarias

Validar el funcionamiento del circuit breaker, backpressure, deduplicador y modelos:
```bash
go test -v -race ./...
```

### Paso 5: Iniciar el Consumidor Resiliente

En una terminal, iniciar el consumidor:
```bash
./bin/consumer \
  -batch-size 400 \
  -workers 60 \
  -cb-failures 4 \
  -cb-cooldown 3s \
  -bp-high 200 \
  -bp-low 30 \
  -idle-timeout 10s
```

### Paso 6: Iniciar el Productor con Ventana de Spike 10x

En otra terminal, lanzar el productor con pacing y ventana de spike:
```bash
./bin/producer \
  -normal-rate 50 \
  -spike-multiplier 10 \
  -spike-delay 10s \
  -spike-duration 125s
```

---

## 4. Parámetros y Banderas de Configuración

### Consumidor (`cmd/consumer`)

| Bandera | Valor por Defecto | Descripción |
| :--- | :--- | :--- |
| `-redis` | `localhost:6379` | Dirección del servidor Redis. |
| `-stream` | `hotsale:events` | Clave del stream en Redis. |
| `-group` | `hotsale_group` | Nombre del grupo de consumidores. |
| `-consumer` | `consumer_resilient` | Identificador del worker consumidor. |
| `-inventory-url` | `http://localhost:8090` | URL base del servicio simulado de inventario. |
| `-workers` | `12` | Número de goroutines concurrentes para verificar inventario. |
| `-cb-failures` | `5` | Fallos consecutivos para abrir el circuit breaker (`CLOSED -> OPEN`). |
| `-cb-cooldown` | `4s` | Tiempo de espera antes de pasar a `HALF_OPEN`. |
| `-cb-probes` | `3` | Peticiones de prueba exitosas en `HALF_OPEN` para cerrar el breaker. |
| `-bp-high` | `400` | Umbral de buffer para activar contrapresión (*High Watermark*). |
| `-bp-low` | `40` | Umbral de buffer para liberar contrapresión (*Low Watermark*). |
| `-buffer-cap` | `50000` | Capacidad máxima en memoria del buffer de reintentos. |
| `-batch-size` | `1000` | Tamaño de lote para `XREADGROUP`. |
| `-idle-timeout` | `10s` | Tiempo de inactividad para considerar concluido el stream tras EOF. |

### Productor (`cmd/producer`)

| Bandera | Valor por Defecto | Descripción |
| :--- | :--- | :--- |
| `-file` | `eventos_hotsale.jsonl` | Ruta al archivo JSONL de eventos. |
| `-redis` | `localhost:6379` | Dirección del broker Redis. |
| `-stream` | `hotsale:events` | Stream de destino. |
| `-normal-rate` | `50` | Ritmo normal sostenido en eventos/segundo. |
| `-spike-multiplier`| `10.0` | Multiplicador del spike (10x el ritmo normal). |
| `-spike-delay` | `15s` | Tiempo de corrida a ritmo normal antes de iniciar el spike. |
| `-spike-duration`| `120s` | Duración del spike sostenido (mínimo 2 minutos). |
| `-batch-size` | `50` | Eventos por batch de `XADD` en pipeline. |
| `-clean` | `false` | Si es true, purga el stream antes de publicar. |

---

## 5. Reporte de Auditoría y Verificación de Datos

Al concluir la ingestión y drenado completo, el consumidor emite una auditoría integral que reporta:

1. **Tabla de Agregados de Negocio**: Órdenes, montos financieros e inventario confirmado por país (AR, CL, CO, MX, PE).
2. **Telemetría del Circuit Breaker**:
   - Total de aperturas (*trip count*).
   - Tiempo total acumulado en estado `OPEN`.
3. **Telemetría de Backpressure**:
   - Activaciones y duración de la contrapresión.
   - Profundidad máxima del buffer de espera (*peak depth*).
   - Eventos residuales al finalizar (debe ser estrictamente **0**).
4. **Telemetría del Servicio de Inventario**:
   - Llamadas exitosas (200 OK), rechazadas (503), y rechazadas en corto circuito (*fast-failed* por el breaker).
5. **Conciliación de Datos (Zero Data Loss)**:
   - Cotejo exacto de eventos procesados vs. esperados, garantizando que el 100% de los eventos legítimos fueron procesados y ningún evento se descartó.
