import { useEffect, useMemo, useRef, useState } from 'react';
import { Drive, MetadataConfig, ScanStatus, UserProfile, ViewState, AppStage } from '../types';
import { MediaService, normalizeRoots, getStatusWsUrls } from '../services/api';
import { useI18n } from '../i18n/i18n';

interface UseSettingsStateParams {
  appStage: AppStage;
  currentUser: UserProfile | null;
  currentView: ViewState;
  metadataConfig: MetadataConfig;
  setMetadataConfig: (cfg: MetadataConfig) => void;
  libraryPaths: string[];
  setLibraryPaths: (paths: string[]) => void;
  profiles: UserProfile[];
  showNotification: (type: 'success' | 'error' | 'info', message: string) => void;
  fetchMedia: (opts?: { reset?: boolean; userProfile?: UserProfile; silent?: boolean }) => Promise<void> | void;
  invalidateMediaCache: () => void;
  setLoading: (val: boolean) => void;
}

export const useSettingsState = ({
  appStage,
  currentUser,
  currentView,
  metadataConfig,
  setMetadataConfig,
  libraryPaths,
  setLibraryPaths,
  profiles,
  showNotification,
  fetchMedia,
  invalidateMediaCache,
  setLoading,
}: UseSettingsStateParams) => {
  const { t } = useI18n();
  const [drives, setDrives] = useState<Drive[]>([]);
  const [scanStatus, setScanStatus] = useState<ScanStatus>({ scanning: false, progress: 0 });
  const [enrichment, setEnrichment] = useState<any | null>(null);
  const [enrichStats, setEnrichStats] = useState<any | null>(null);
  const [wsConnected, setWsConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const [serverOffline, setServerOffline] = useState(false);
  const serverOfflineRef = useRef(false);
  const prevScanRunningRef = useRef(false);
  const prevEnrichRunningRef = useRef<boolean>(false);
  const processRunningRef = useRef(false);
  const [migrationRunning, setMigrationRunning] = useState<boolean>(false);
  const [migrationError, setMigrationError] = useState<string | null>(null);
  const lastWatchTsRef = useRef<number>(0);
  const watchRefreshTimerRef = useRef<number | null>(null);

  const enrichmentRunning = useMemo(
    () => !!((enrichment as any)?.running || (enrichStats as any)?.running),
    [enrichment, enrichStats],
  );

  const fetchSettingsData = async () => {
    setLoading(true);
    try {
      const drivesData = await MediaService.getDrives();
      setDrives(drivesData);
      const status = await MediaService.getScanStatus();
      setScanStatus(status);
      setEnrichment((status as any).enrichment || null);
      try {
        const es = await MediaService.getEnrichStatus();
        setEnrichStats(es || null);
        if (es) setEnrichment((prev) => ({ ...(prev || {}), ...(es || {}) }));
      } catch {
        // ignore
      }

      const roots = await MediaService.getRoots();
      setLibraryPaths(normalizeRoots(roots));
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const handleStartEnrich = async () => {
    try {
      const res = await MediaService.startEnrichment();
      if (res && res.ok && res.data && res.data.started) {
        showNotification('success', 'Se inició el proceso de enriquecimiento');
        try {
          const es = await MediaService.getEnrichStatus();
          setEnrichStats(es || null);
          if (es) setEnrichment((prev) => ({ ...(prev || {}), ...(es || {}) }));
        } catch {}
      } else if (res && res.ok && res.data && !res.data.started) {
        showNotification('info', 'El proceso de enriquecimiento no se inició (¿ya estaba en ejecución?)');
      } else {
        showNotification('error', 'No se pudo iniciar el proceso de enriquecimiento');
      }
    } catch {
      showNotification('error', 'No se pudo iniciar el proceso de enriquecimiento');
    }
  };

  const handleStartScan = async (pathsToScan = libraryPaths) => {
    if (scanStatus.scanning) {
      setScanStatus({ scanning: false, progress: 0, currentFile: undefined });
      showNotification('info', 'Escaneo cancelado');
      return;
    }
    setScanStatus({ scanning: true, progress: 0, currentFile: 'Inicializando...' });
    if (!pathsToScan || pathsToScan.length === 0) {
      showNotification('info', 'No hay rutas configuradas para escanear.');
      setScanStatus({ scanning: false, progress: 0 });
      return;
    }

    try {
      setMigrationRunning(true);
      setMigrationError(null);
      showNotification('info', 'Aplicando migraciones de la base de datos (si es necesario)...');

      const startRes: any = await MediaService.startScan();
      const statusStr =
        (startRes && startRes.status) || (startRes && startRes.raw && startRes.raw.status) || null;
      if (statusStr === 'migration_failed' || (startRes && startRes.raw && startRes.raw.status === 'migration_failed')) {
        setScanStatus({ scanning: false, progress: 0 });
        const detail = startRes.detail || (startRes.raw && startRes.raw.detail) || 'see server logs';
        setMigrationError(detail);
        setMigrationRunning(false);
        showNotification('error', `Falló la migración de la base de datos: ${detail}`);
        return;
      }

      if (statusStr === 'already_running') {
        showNotification('info', 'El escaneo ya está en ejecución');
      }
      setMigrationRunning(false);

      const poll = async () => {
        try {
          const status = await MediaService.getScanStatus();
          const progressVal =
            typeof status.progress === 'number'
              ? status.progress
              : status.total && status.processed
                ? Math.round((status.processed / status.total) * 100)
                : 0;
          setScanStatus({
            scanning: !!(status && status.scanning) || status.status !== 'idle',
            progress: progressVal,
            currentFile: (status.current || status.currentFile) as any,
            processed: (status as any).processed,
            total: (status as any).total,
          } as any);
          setEnrichment((status as any).enrichment || null);
          try {
            const es = await MediaService.getEnrichStatus();
            setEnrichStats(es || null);
            if (es) setEnrichment((prev) => ({ ...(prev || {}), ...(es || {}) }));
          } catch {
            // ignore
          }

          if (!status || status.status === 'idle' || !(status.scanning || status.status === 'scanning')) {
            setScanStatus((prev) => ({ ...prev, scanning: false, progress: 100 } as any));
            invalidateMediaCache();
            setTimeout(() => fetchMedia({ reset: true, silent: true }), 800);
            try {
              const es = await MediaService.getEnrichStatus();
              setEnrichStats(es || null);
              if (es) setEnrichment((prev) => ({ ...(prev || {}), ...(es || {}) }));
              if (!es || !es.running) {
                await handleStartEnrich();
              }
            } catch {
              // ignore
            }
            setMigrationRunning(false);
          } else {
            setTimeout(poll, 1000);
          }
        } catch (e) {
          console.warn('Error polling scan status', e);
          setTimeout(poll, 2000);
        }
      };

      poll();
    } catch (e) {
      console.error('Failed to start scan', e);
      setScanStatus({ scanning: false, progress: 0 });
      showNotification('error', 'No se pudo iniciar el escaneo');
    }
  };

  const handleAddPath = (path: string) => {
    (async () => {
      try {
        const roots = await MediaService.addRoot(path);
        setLibraryPaths(normalizeRoots(roots));
        showNotification('success', `Ruta agregada: ${path}`);
        try {
          await MediaService.saveAppConfig({
            setupComplete: true,
            profiles,
            metadata: metadataConfig,
            media_roots: roots,
          });
        } catch (e) {
          console.warn('Failed to persist app-config after addRoot', e);
        }
      } catch {
        showNotification('error', 'No se pudo agregar la ruta');
      }
    })();
  };

  const handleRemovePath = (path: string) => {
    (async () => {
      try {
        const roots = await MediaService.removeRoot(path);
        setLibraryPaths(normalizeRoots(roots));
        showNotification('info', 'Ruta eliminada');
        try {
          await MediaService.saveAppConfig({
            setupComplete: true,
            profiles,
            metadata: metadataConfig,
            media_roots: roots,
          });
        } catch (e) {
          console.warn('Failed to persist app-config after removeRoot', e);
        }
      } catch {
        showNotification('error', 'No se pudo eliminar la ruta');
      }
    })();
  };

  const handleUpdateMetadataConfig = (config: MetadataConfig) => {
    setMetadataConfig(config);
    showNotification('success', 'Configuración de metadatos guardada');
    (async () => {
      try {
        await MediaService.saveAppConfig({
          setupComplete: true,
          profiles,
          metadata: config,
          media_roots: libraryPaths,
        });
      } catch (e) {
        console.warn('Failed to persist metadata config', e);
      }
    })();
  };

  // Real-time status via WebSocket.
  useEffect(() => {
    if (appStage !== 'app') return;
    if (typeof window === 'undefined') return;

    let cancelled = false;
    let reconnectTimer: number | null = null;
    let attempt = 0;

    const buildCandidates = (): string[] => getStatusWsUrls();

    const applySnapshot = (snap: any) => {
      if (!snap || typeof snap !== 'object') return;
      const scan = snap.scan || null;
      const enrich = snap.enrich || null;
      const counts = snap.enrich_counts || null;

      if (scan && typeof scan === 'object') {
        const total = Number.isFinite(scan.total) ? Number(scan.total) : undefined;
        const processed = Number.isFinite(scan.processed) ? Number(scan.processed) : undefined;
        const status = typeof scan.status === 'string' ? scan.status : undefined;
        const scanning = !!(scan.scanning || status === 'scanning' || status === 'queued');
        const progress =
          typeof scan.progress === 'number'
            ? scan.progress
            : total && processed
              ? Math.round((processed / total) * 100)
              : scanning
                ? 0
                : 0;
        const currentFile = (scan.current || scan.currentFile || scan.current_path || scan.current_item) as any;
        setScanStatus((prev) => ({
          ...prev,
          scanning,
          progress,
          total,
          processed,
          currentFile: typeof currentFile === 'string' ? currentFile : prev.currentFile,
          status,
        }));
      }

      if (enrich && typeof enrich === 'object') {
        setEnrichment((prev) => ({ ...(prev || {}), ...(enrich || {}) }));
      }
      if (counts && typeof counts === 'object') {
        setEnrichStats((prev) => ({ ...(prev || {}), ...(counts || {}) }));
        setEnrichment((prev) => ({ ...(prev || {}), ...(counts || {}) }));
      }
    };

    const connect = () => {
      if (cancelled) return;
      const urls = buildCandidates();
      if (!urls.length) return;

      const url = urls[Math.min(attempt, urls.length - 1)];
      attempt += 1;

      try {
        const ws = new WebSocket(url);
        wsRef.current = ws;

        ws.onopen = () => {
          if (cancelled) return;
          attempt = 0;
          setWsConnected(true);
        };
        ws.onclose = () => {
          if (cancelled) return;
          setWsConnected(false);
          if (reconnectTimer) window.clearTimeout(reconnectTimer);
          const backoff = Math.min(8000, 600 + Math.round(Math.random() * 500) + attempt * 700);
          reconnectTimer = window.setTimeout(connect, backoff);
        };
        ws.onerror = () => {
          try {
            ws.close();
          } catch {
            // ignore
          }
        };
        ws.onmessage = (ev) => {
          if (cancelled) return;
          try {
            const snap = JSON.parse(ev.data);
            applySnapshot(snap);
          } catch {
            // ignore
          }
        };
      } catch {
        // ignore
      }
    };

    connect();
    return () => {
      cancelled = true;
      setWsConnected(false);
      if (reconnectTimer) window.clearTimeout(reconnectTimer);
      try {
        wsRef.current?.close();
      } catch {
        // ignore
      }
      wsRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [appStage]);

  // Heartbeat: detect backend offline.
  useEffect(() => {
    if (typeof window === 'undefined') return;

    let cancelled = false;
    let inFlight = false;
    let failStreak = 0;
    let okStreak = 0;

    const setOfflineStable = (offline: boolean) => {
      if (serverOfflineRef.current === offline) return;
      serverOfflineRef.current = offline;
      setServerOffline(offline);
    };

    const tick = async () => {
      if (inFlight) return;
      inFlight = true;
      const ac = new AbortController();
      const t = window.setTimeout(() => ac.abort(), 5000);
      try {
        const res = await fetch('/api/health', { signal: ac.signal });
        if (cancelled) return;
        const ok = !!(res && res.ok);
        if (ok) {
          okStreak += 1;
          failStreak = 0;
          if (serverOfflineRef.current && okStreak >= 1) setOfflineStable(false);
        } else {
          failStreak += 1;
          okStreak = 0;
          if (!serverOfflineRef.current && failStreak >= 3) setOfflineStable(true);
        }
      } catch {
        if (cancelled) return;
        failStreak += 1;
        okStreak = 0;
        if (!serverOfflineRef.current && failStreak >= 3) setOfflineStable(true);
      } finally {
        window.clearTimeout(t);
        inFlight = false;
      }
    };

    void tick();
    const id = window.setInterval(() => void tick(), 6000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, []);

  // Refresh grid once when scan transitions to idle.
  useEffect(() => {
    const running = !!scanStatus.scanning;
    const wasRunning = !!prevScanRunningRef.current;
    prevScanRunningRef.current = running;
    if (wasRunning && !running && appStage === 'app' && !serverOfflineRef.current) {
      invalidateMediaCache();
      void fetchMedia({ reset: true, silent: true });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scanStatus.scanning]);

  // Notify once when scan + enrichment finishes.
  useEffect(() => {
    if (appStage !== 'app') return;
    const runningNow = !!scanStatus.scanning || !!enrichmentRunning;
    const wasRunning = !!processRunningRef.current;
    processRunningRef.current = runningNow;
    if (wasRunning && !runningNow && !serverOfflineRef.current) {
      try {
        showNotification('success', t('notif_library_updated'));
      } catch {}
      invalidateMediaCache();
      void fetchMedia({ reset: true, silent: true });
    }
  }, [appStage, scanStatus.scanning, enrichmentRunning]);

  // Keep enrichment status in sync while scan/enrichment is running (or while viewing settings).
  useEffect(() => {
    if (appStage !== 'app') return;

    const shouldPoll = !wsConnected && (currentView === 'settings' || scanStatus.scanning || enrichmentRunning);
    if (!shouldPoll) return;

    let cancelled = false;
    const tick = async () => {
      try {
        const es = await MediaService.getEnrichStatus();
        if (cancelled) return;
        setEnrichStats(es || null);
        if (es) setEnrichment((prev) => ({ ...(prev || {}), ...(es || {}) }));

        const runningNow = !!(es && es.running);
        prevEnrichRunningRef.current = runningNow;
      } catch {
        // ignore
      }
    };

    void tick();
    const id = window.setInterval(() => void tick(), 5000);

    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, [appStage, currentView, scanStatus.scanning, enrichmentRunning, wsConnected]);

  // Watcher notifications (new/changed media files) for manager profile only.
  useEffect(() => {
    if (appStage !== 'app') return;
    if (!currentUser) return;
    if (!currentUser.isManager) return;

    let cancelled = false;
    const tick = async () => {
      try {
        const st: any = await MediaService.getScanStatus();
        if (cancelled) return;
        const w = st && (st as any).watch;
        const ts = w && typeof w.ts === 'number' ? Number(w.ts) : 0;
        const p = w && w.path ? String(w.path) : '';
        if (ts && ts > (lastWatchTsRef.current || 0) && p) {
          lastWatchTsRef.current = ts;
          try {
            if (watchRefreshTimerRef.current) window.clearTimeout(watchRefreshTimerRef.current);
            watchRefreshTimerRef.current = window.setTimeout(() => {
              invalidateMediaCache();
              void fetchMedia({ reset: true, userProfile: currentUser, silent: true });
            }, 1200);
          } catch {
            // ignore
          }
        }
      } catch {
        // ignore
      }
    };

    void tick();
    const id = window.setInterval(() => void tick(), 5000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [appStage, currentUser?.id]);

  return {
    drives,
    scanStatus,
    enrichment,
    enrichStats,
    serverOffline,
    migrationRunning,
    migrationError,
    enrichmentRunning,
    fetchSettingsData,
    handleStartScan,
    handleStartEnrich,
    handleAddPath,
    handleRemovePath,
    handleUpdateMetadataConfig,
  };
};
