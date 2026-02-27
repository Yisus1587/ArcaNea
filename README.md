# ArcaNea — Local Media Server (Frontend + Backend)

ArcaNea es una app para gestionar y reproducir una biblioteca multimedia local (peliculas, series, anime) con:
- Backend en Python (FastAPI) para escaneo, base de datos, metadatos y streaming.
- Frontend en React + Vite para navegacion, reproductor y gestion.

**Estado:** en desarrollo (uso local funcional, en evolucion).

---

**Estructura**
- `src/` — Backend (FastAPI, servicios, APIs).
- `arcanea-media-server/` — Frontend (Vite + React + TypeScript).
- `data/` — configuracion y cache (ej. `app_config.json`).

**Entradas**
- Backend: `src/api/asgi.py`
- Runner dev: `dev_run.py`
- Frontend: `arcanea-media-server/package.json`

---

## Requisitos
- Python 3.11+
- Node.js + npm

## Instalacion

Backend:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Frontend:
```bash
cd arcanea-media-server
npm install
```

## Variables utiles
- `ARCANEA_SERVE_FRONTEND=1` — sirve el frontend construido desde el backend.
- `ARCANEA_MEDIA_ROOT` — ruta inicial sugerida para escaneo.
- `ARCANEA_STARTUP_SCAN`, `ARCANEA_WATCH` — comportamiento al iniciar.
- `TMDB_API_KEY` — clave TMDB (opcional, recomendado).

## Ejecucion (dev)

Backend:
```bash
python -m uvicorn src.api.asgi:app --host 127.0.0.1 --port 9800 --reload
```

Frontend:
```bash
cd arcanea-media-server
npm run dev
```

URLs:
- Frontend: http://localhost:9587/
- Backend:  http://localhost:9800/

## Comando rapido (build + server)
```powershell
python dev_run.py --host 127.0.0.1 --port 9800
```

`dev_run.py`:
- `npm run build` (si no usas `--skip-build`)
- `ARCANEA_SERVE_FRONTEND=1`
- `uvicorn src.api.asgi:app`

---

## Flujo de uso (resumen)
1. Inicia la app y crea el perfil de gestion (PIN requerido).
2. Agrega rutas de biblioteca.
3. Se ejecuta escaneo; si hay TMDB key, se enriquece.
4. Usa "Revisar sin match" o "Corregir Match (Manual)" si hay errores.
5. Reproduce contenido desde la UI.

## Seguridad
- Endpoints de administracion (scan/enrich/fs/logs/credentials) requieren token admin.
- Evita agregar rutas sensibles como media_roots.

## Notas
- Para acceso LAN, inicia backend con `0.0.0.0` y revisa firewall.

---
