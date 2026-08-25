# Runner Client — Guía de arquitectura

Documento de referencia para continuar el desarrollo del cliente gRPC Runner.

## Qué es este proyecto

Cliente Java que mantiene una sesión bidireccional con **API EXTERNOS** (servidor gRPC Spring Boot). Implementa el lifecycle completo del Runner: autenticación, handshake, sesión activa con heartbeats y reconexión automática.

---

## Cómo ejecutar

```bash
mvn package -DskipTests
java --enable-native-access=ALL-UNNAMED -jar target/runner-1.0.0.jar
```

Entry point: `com.maximus.runner.RunnerApplication`

La configuración se carga con `RunnerConfigLoader`: **host** y **puerto** son obligatorios; el resto es opcional y usa valores por defecto si no se indica.

### Modo 1 — Argumentos en línea de comandos (recomendado)

Host y puerto pueden ir como flags o como argumentos posicionales (en ese orden):

```bash
# Posicional: host y puerto
java -jar target/runner-1.0.0.jar 45.55.104.90 9090

# Flags explícitos
java -jar target/runner-1.0.0.jar --host=45.55.104.90 --port=9090
```

Campos opcionales (todos aceptan `--clave=valor` o `--clave valor`):

| Flag | Campo | Default |
|------|-------|---------|
| `--credential` | Credencial de autenticación | `runner-credential` |
| `--runner-id` | Identificador del runner | `runner-1` |
| `--runner-version` | Versión reportada en handshake | `1.0.0` |
| `--protocol-version` | Versión de protocolo | `1` |
| `--reconnect-initial-ms` | Backoff inicial de reconexión | `1000` |
| `--reconnect-max-ms` | Backoff máximo de reconexión | `30000` |
| `--heartbeat-interval-ms` | Intervalo de heartbeat (fallback) | `5000` |

Ejemplo con opcionales:

```bash
java -jar target/runner-1.0.0.jar \
  --host=45.55.104.90 \
  --port=9090 \
  --credential=mi-credencial \
  --runner-id=runner-prod-1
```

Si pasas host y puerto por CLI, **no se muestran prompts** para esos campos. Los opcionales que falten tampoco piden entrada: se aplican los defaults.

### Modo 2 — Consola interactiva

Si no pasas host/puerto (o solo uno de los dos), el arranque pregunta por consola:

```
==================================================
[RUNNER] Configuración
==================================================
Servidor (host) [requerido]: 45.55.104.90
Puerto [requerido]: 9090
Credencial [runner-credential]: 
Runner ID [runner-1]: 
Versión del runner [1.0.0]: 
Versión de protocolo [1]: 
Backoff inicial (ms) [1000]: 
Backoff máximo (ms) [30000]: 
Intervalo heartbeat (ms) [5000]: 
```

- Campos marcados `[requerido]`: deben tener valor; si están vacíos, se vuelve a pedir.
- Campos con `[default]`: pulsa **Enter** para usar el valor entre corchetes.
- Puedes mezclar: por ejemplo `--host=45.55.104.90` por CLI y el puerto se pide en consola.

### Modo 3 — Desarrollo con Maven

```bash
mvn exec:java -Dexec.mainClass=com.maximus.runner.RunnerApplication \
  -Dexec.args="45.55.104.90 9090"
```

---

## Estructura de paquetes

```
com.maximus.runner/
│
├── RunnerApplication.java          # main — arranca el Singleton
│
├── configuration/
│   ├── RunnerConfig.java           # record inmutable de configuración
│   └── RunnerConfigLoader.java     # carga desde CLI y/o consola interactiva
│
├── domain/                         # reglas de negocio puras
│   ├── RunnerState.java            # enum de estados del lifecycle
│   ├── RunnerStateMachine.java     # transiciones validadas
│   ├── SessionContext.java         # datos de sesión post-handshake
│   ├── StateTransition.java        # VO (from, to, reason)
│   └── StateTransitionListener.java
│
├── application/                    # casos de uso y orquestación
│   ├── RunnerService.java          # Singleton — punto de entrada
│   │
│   ├── lifecycle/
│   │   ├── RunnerLifecycle.java    # connect, auth, handshake, reconnect
│   │   ├── SessionManager.java     # loop ACTIVE (heartbeat, status, health)
│   │   ├── ReconnectPolicy.java    # backoff exponencial
│   │   ├── ActiveSessionHandler.java
│   │   └── LifecycleContext.java
│   │
│   ├── monitoring/
│   │   ├── ServerHealth.java
│   │   ├── ServerHealthMonitor.java
│   │   └── collector/
│   │       ├── SystemHealthCollector.java
│   │       ├── DatabaseHealthCollector.java   # stub
│   │       └── NetworkLatencyCollector.java
│   │
│   └── port/
│       └── RunnerConnection.java   # abstracción de transporte gRPC
│
└── infrastructure/                 # detalles técnicos
    └── grpc/
        ├── GrpcSession.java        # implements RunnerConnection
        └── GrpcSessionOpenException.java
```

### Protobuf (generado en build)

```
src/main/protobuf/
├── runner.proto      # servicio Connect + mensajes envelope
├── request.proto     # AuthenticateRequest, HandshakeRequest, HealthStatus, etc.
└── response.proto    # AuthenticationResponse, HandshakeResponse, Command
```

Clases Java generadas en: `target/generated-sources/protobuf/java/`

---

## Patrones de diseño

| Patrón | Dónde | Propósito |
|--------|-------|-----------|
| **Singleton** | `RunnerService` | Una instancia por JVM |
| **Facade** | `RunnerService` | API simple: `start()` / `shutdown()` |
| **State Machine** | `RunnerStateMachine` | Transiciones del lifecycle validadas |
| **Value Object** | `RunnerConfig`, `SessionContext`, `StateTransition`, `ServerHealth` | Datos inmutables |
| **Port & Adapter** | `RunnerConnection` ← `GrpcSession` | Desacoplar lógica de gRPC |
| **Observer (ligero)** | `StateTransitionListener` | Log de cambios de estado |
| **DDD light** | Paquetes `domain`, `application`, `infrastructure` | Separación por responsabilidad |

---

## Flujo de arranque

```
RunnerApplication.main()
  │
  ├─ RunnerConfigLoader.load(args)
  ├─ RunnerService.initialize(config)
  ├─ shutdown hook → RunnerService.getInstance().shutdown()
  └─ RunnerService.getInstance().start()
       │
       └─ RunnerLifecycle.run()
            ├─ SessionManager (ActiveSessionHandler)
            └─ loop reconexión
```

### Composición en `RunnerService`

```java
SessionManager sessionManager = new SessionManager(config);
RunnerLifecycle lifecycle = new RunnerLifecycle(config, sessionManager);
sessionManager.attach(lifecycle);  // LifecycleContext para sync y disconnect
```

---

## Máquina de estados

```
PROVISIONED → DISCONNECTED → AUTHENTICATING → AUTHENTICATED → HANDSHAKING → ACTIVE
                  ↑___________________________________________________________|
                         (error / auth rejected / handshake rejected)
```

| Estado | Quién lo gestiona | Qué ocurre |
|--------|-------------------|------------|
| `PROVISIONED` | `RunnerStateMachine` | Estado inicial del enum |
| `DISCONNECTED` | `RunnerLifecycle` | Espera backoff, llama `attemptConnection()` |
| `AUTHENTICATING` | `RunnerLifecycle` | Envía `AuthenticateRequest` |
| `AUTHENTICATED` | `RunnerLifecycle` | Envía `HandshakeRequest` → `HANDSHAKING` |
| `HANDSHAKING` | `RunnerLifecycle` | Espera `HandshakeResponse` |
| `ACTIVE` | `SessionManager` | Loop de payloads operativos |

Transiciones inválidas lanzan `IllegalStateException`. Cada cambio se loguea:

```
[RUNNER][STATE] DISCONNECTED → AUTHENTICATING (connect)
```

---

## Secuencia gRPC (payloads en orden)

```
Connect() stream
  → AuthenticateRequest
  ← AuthenticationResponse
  → HandshakeRequest
  ← HandshakeResponse

[While ACTIVE]
  → Heartbeat
  → StatusUpdate (READY)
  → HealthUpdate
  ← Command (reactivo)
  → CommandResult
```

### Dónde se implementa cada payload

| Payload | Clase | Método |
|---------|-------|--------|
| `AuthenticateRequest` | `RunnerLifecycle` | `sendAuthentication()` |
| `HandshakeRequest` | `RunnerLifecycle` | `sendHandshake()` |
| `Heartbeat` | `SessionManager` | `sendHeartbeat()` |
| `StatusUpdate` | `SessionManager` | `sendStatusUpdate()` |
| `HealthUpdate` | `SessionManager` | `sendHealthUpdate()` → `ServerHealthMonitor` |
| `CommandResult` | `SessionManager` | `sendCommandResult()` |

---

## Responsabilidad por clase

### `RunnerApplication`
- Carga config, inicializa Singleton, registra shutdown hook.
- No contiene lógica de negocio.

### `RunnerService` (Singleton)
- Único punto de entrada al lifecycle.
- `initialize(config)` — una sola vez por JVM.
- `getInstance().start()` / `shutdown()`.

### `RunnerLifecycle`
- Loop principal en hilo main.
- Abre conexión gRPC, auth, handshake.
- Implementa `RunnerConnection.ConnectionListener` y `LifecycleContext`.
- Delega sesión ACTIVE a `SessionManager` vía `ActiveSessionHandler`.
- Gestiona `disconnect()` y `ReconnectPolicy`.

### `SessionManager`
- Hilo dedicado `runner-active-session`.
- En cada ciclo: Heartbeat → StatusUpdate → HealthUpdate.
- Procesa `Command` entrante (responde `CommandResult` con `not implemented`).
- Usa `lifecycleContext.lifecycleLock()` para sincronizar con callbacks gRPC.

### `GrpcSession`
- Implementa `RunnerConnection`.
- Crea channel Netty, espera `READY`, abre stream `Connect()`.
- `send()` sincronizado, `close()` limpia stream y channel.

### `ServerHealthMonitor`
- Orquesta collectors y produce `HealthUpdate`.
- `SystemHealthCollector`: CPU, RAM, disco, red, procesos (JMX).
- `NetworkLatencyCollector`: latencia TCP al servidor.
- `DatabaseHealthCollector`: **stub** — pendiente config JDBC.

### `RunnerStateMachine`
- Mapa de transiciones permitidas.
- Emite `StateTransition` a listeners.

---

## Configuración

`RunnerConfig` (`configuration/RunnerConfig.java`) es un record inmutable. Los valores se cargan con `RunnerConfigLoader.load(args)`:

| Campo | Obligatorio | Default | Notas |
|-------|-------------|---------|-------|
| `serverHost` | **Sí** | — | IP o hostname del API EXTERNOS |
| `serverPort` | **Sí** | — | Puerto gRPC (1–65535) |
| `credential` | No | `runner-credential` | Debe existir en servidor/MongoDB |
| `runnerId` | No | `runner-1` | Identificador del runner |
| `runnerVersion` | No | `1.0.0` | Versión reportada en handshake |
| `protocolVersion` | No | `1` | Versión de protocolo |
| `initialReconnectDelayMs` | No | `1000` | Backoff inicial |
| `maxReconnectDelayMs` | No | `30000` | Backoff máximo |
| `fallbackHeartbeatIntervalMs` | No | `5000` | Si handshake no define intervalo |

`RunnerConfig.defaults()` conserva los mismos valores por defecto y se usa internamente como base al cargar la config.

---

## Hilos y concurrencia

| Hilo | Clase | Rol |
|------|-------|-----|
| `main` | `RunnerLifecycle` | Loop reconexión |
| `grpc-default-executor-*` | gRPC callbacks | `onResponse`, `onError`, `onCompleted` |
| `runner-active-session` | `SessionManager` | Loop heartbeat/status/health |

Sincronización: `RunnerLifecycle.lifecycleLock` — compartido entre lifecycle y session manager.

---

## Build

- **Java 25**, Maven, gRPC 1.83, Protobuf 3.25
- Protobuf se genera en fase `generate-sources` (exec-maven-plugin)
- JAR ejecutable: `maven-shade-plugin` → `target/runner-1.0.0.jar`

---