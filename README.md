# ArcaNea - Local Media Server (Frontend + Backend)

Servidor multimedia local (LAN) con interfaz web y backend en FastAPI.
Pensado para correr en tu PC y usarse desde la misma maquina o desde el celular dentro de la misma red.

Estado: en desarrollo. La base funciona, pero el proyecto sigue evolucionando.

---

## Objetivo
- Local first: todo corre en tu PC, sin depender de servicios externos.
- Rendimiento: UI rapida para bibliotecas grandes.
- Control: perfiles, ajustes y correccion manual de metadatos.
- Escalable: arquitectura preparada para crecer con mas proveedores y herramientas.

---

## Que incluye
- Backend (FastAPI): escaneo, base de datos, enriquecimiento y streaming.
# ArcaNea — Servidor multimedia local (Frontend + Backend)

ArcaNea es una aplicación para gestionar y reproducir una biblioteca multimedia local (películas, series, anime) con:
- Backend en Python (FastAPI) responsable de escaneo, enriquecimiento, APIs y (opcional) servir el frontend.
- Frontend en React + Vite para navegación, reproductor y gestión.

Este README explica cómo instalar, ejecutar y dónde encontrar los recursos principales.

**Estado:** en desarrollo — funcional para uso local, pero en evolución.

**Estructura principal**
- `src/` — Backend (FastAPI, escáner, servicios, proveedores, APIs).
- `arcanea-media-server/` — Frontend (Vite + React + TypeScript).
- `data/` — datos de configuración y cachés (ej. `app_config.json`).

Relevantes:
- Backend entry: `src/api/asgi.py` (punto ASGI para `uvicorn`).
- Dev runner (build frontend + lanzar backend): `dev_run.py`.
- Frontend package: `arcanea-media-server/package.json`.

Requisitos
- Python 3.11+
- Node.js + npm (para el frontend)

Dependencias (seleccionadas)
- Backend: FastAPI, Uvicorn, SQLAlchemy, Watchdog, Pydantic, httpx, etc. (ver `requirements.txt`).
- Frontend: React 19, Vite, TypeScript (ver `arcanea-media-server/package.json`).

Instalación

Backend
1. Crear y activar entorno virtual:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Instalar dependencias:

```powershell
pip install -r requirements.txt
```

Frontend

```bash
cd arcanea-media-server
npm install
```

Variables útiles:
- `ARCANEA_SERVE_FRONTEND=1` — indica al backend servir los archivos estáticos del frontend si están construidos.
- `ARCANEA_MEDIA_ROOT` — ruta inicial sugerida para escaneo.
- `ARCANEA_STARTUP_SCAN`, `ARCANEA_STARTUP_ENRICH`, `ARCANEA_WATCH` — controles de comportamiento al iniciar.

Ejecución (desarrollo)

Backend (uvicorn):

```bash
python -m uvicorn src.api.asgi:app --host 127.0.0.1 --port 9800 --reload
```

Frontend (desarrollo):

```bash
cd arcanea-media-server
npm run dev
```

URLs por defecto (dev):
- Frontend: http://localhost:9587/ (Vite)
- Backend:  http://localhost:9800/

Comando útil (build frontend y servir backend) — `dev_run.py` simplifica este flujo:

```powershell
python dev_run.py --host 127.0.0.1 --port 9800
```

`dev_run.py` hace:
- ejecuta `npm run build` dentro de `arcanea-media-server` (a menos que pases `--skip-build`).
- establece `ARCANEA_SERVE_FRONTEND=1` por defecto (puedes usar `--no-serve-frontend`).
- lanza `uvicorn` apuntando a `src.api.asgi:app`.

Construcción / Release

1. Construir frontend:

```bash
cd arcanea-media-server
npm run build
```

2. Servir desde backend (producción local):

```powershell
set ARCANEA_SERVE_FRONTEND=1
python -m uvicorn src.api.asgi:app --host 0.0.0.0 --port 9800
```

Puntos clave del proyecto (recursos)
- Frontend source: `arcanea-media-server/src/` (componentes React principales en `components/`).
- Frontend config: `arcanea-media-server/package.json`, `index.html`, `vite.config.ts`.
- Backend entry: `src/api/asgi.py` (ASGI app). APIs en `src/api/` y lógica en `src/services/`.
- Runner de escaneo en background: `dev_run.py` para tareas de desarrollo; `src/watcher/service.py` y `src/scanner/scanner.py` contienen el watcher y scanner.
- Config por defecto / datos: `data/app_config.json`.

Flujo de uso rápido
1. Iniciar backend y frontend en modo dev.
2. Abrir la UI, crear/seleccionar un perfil administrador (PIN requerido).
3. Añadir rutas de medios en la UI (Settings / Folder picker) o via `app_config.json`.
4. Ejecutar escaneo y, si procede, enriquecimiento (TMDB/Jikan).
5. Reproducir desde la UI; usar 'Manual Mapping' si hay coincidencias incorrectas.

Notas y buenas prácticas
- Mantén el `media_roots` apuntando solo a carpetas de medios confiables para evitar exponer otros archivos.
- Para acceso desde otros dispositivos en la LAN, inicia el backend en `0.0.0.0` y asegúrate de las reglas de firewall.
- Si planeas publicar, añade un archivo `LICENSE` y revisa dependencias para licencias compatibles.

Soporte y próximos pasos
- El código está estructurado para añadir más proveedores y mejoras de enriquecimiento.
- Si quieres que revise y documente endpoints específicos o escriba scripts de despliegue (Docker/WSL), dime y lo agrego.

---

Si quieres, genero un apartado adicional con ejemplos de llamadas a la API o un `docker-compose.yml` mínimo.
- Logs, backup, filesystem, scan/enrich y credenciales estan protegidos por admin token.
