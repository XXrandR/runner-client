# Runner Client — Guía de arquitectura

Documento de referencia para continuar el desarrollo del cliente gRPC Runner.

## Qué es este proyecto

Cliente Java que mantiene una sesión bidireccional con **API EXTERNOS** (servidor gRPC Spring Boot). Implementa el lifecycle completo del Runner: autenticación, handshake, sesión activa con heartbeats y reconexión automática.

---

## Cómo ejecutar

```bash
mvn package
java -jar target/runner-1.0.0.jar
```

Entry point: `com.maximus.runner.RunnerApplication`

---

## Estructura de paquetes

```
com.maximus.runner/
│
├── RunnerApplication.java          # main — arranca el Singleton
│
├── configuration/
│   └── RunnerConfig.java           # constantes de conexión y runner
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
  ├─ RunnerConfig.defaults()
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

`RunnerConfig.defaults()` en `configuration/RunnerConfig.java`:

| Campo | Valor actual | Notas |
|-------|--------------|-------|
| `serverHost` | `45.55.104.90` | IP del API EXTERNOS |
| `serverPort` | `9090` | Puerto gRPC |
| `credential` | `runner-credential` | Debe existir en servidor/MongoDB |
| `runnerId` | `runner-1` | Identificador del runner |
| `runnerVersion` | `1.0.0` | Versión reportada en handshake |
| `protocolVersion` | `1` | Versión de protocolo |
| `initialReconnectDelayMs` | `1_000` | Backoff inicial |
| `maxReconnectDelayMs` | `30_000` | Backoff máximo |
| `fallbackHeartbeatIntervalMs` | `5_000` | Si handshake no define intervalo |

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