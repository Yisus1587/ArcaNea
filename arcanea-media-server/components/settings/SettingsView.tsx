import React, { useState, useEffect, useRef } from 'react';
import {
  HardDrive, FolderPlus, Trash2, Database, ScanLine, Loader2,
  RefreshCw, CheckCircle, AlertCircle, Plus, Save, User, Users, Pencil, X
} from 'lucide-react';
import { Button } from '../ui/Button';
import { Drive, ScanStatus, MetadataConfig, UserProfile } from '../../types';
import { useI18n } from '../../i18n/i18n';
import { FolderPickerModal } from './FolderPickerModal';
import { HelpModal } from './HelpModal';
import { NoMatchModal } from './NoMatchModal';

interface SettingsViewProps {
  drives: Drive[];
  libraryPaths: string[];
  scanStatus: ScanStatus;
  metadataConfig: MetadataConfig;
  profiles: UserProfile[];
  onAddPath: (path: string) => void;
  onRemovePath: (path: string) => void;
  onUpdateConfig: (config: MetadataConfig) => void;
  onStartScan: () => void;
  onRefresh?: () => void;
  onAddProfile: (profile: Omit<UserProfile, 'id'>) => void;
  onUpdateProfile: (profile: UserProfile) => void;
  onDeleteProfile: (id: string) => void;
  enrichment?: any;
  migrationRunning?: boolean;
  migrationError?: string | null;
  onStartEnrich?: () => void;
}

const AVATAR_COLORS = [
  'bg-indigo-600', 'bg-purple-600', 'bg-pink-600', 'bg-red-600',
  'bg-orange-600', 'bg-yellow-500', 'bg-green-600', 'bg-blue-600', 'bg-slate-600'
];

export const SettingsView: React.FC<SettingsViewProps> = ({
  drives,
  libraryPaths,
  scanStatus,
  metadataConfig,
  profiles,
  onAddPath,
  onRemovePath,
  onUpdateConfig,
  onStartScan,
  onRefresh,
  onAddProfile,
  onUpdateProfile,
  onDeleteProfile,
  enrichment,
  migrationRunning,
  migrationError,
  onStartEnrich
}) => {
  const { lang, setLang, t } = useI18n();
  const [newPathInput, setNewPathInput] = useState('');
  const [tmdbConfigured, setTmdbConfigured] = useState<boolean | null>(null);
  const [tmdbTestResult, setTmdbTestResult] = useState<string | null>(null);
  const [showTmdbCredentials, setShowTmdbCredentials] = useState(false);
  const [tmdbApiKeyInput, setTmdbApiKeyInput] = useState('');
  const [tmdbAccessTokenInput, setTmdbAccessTokenInput] = useState('');
  const [tmdbUseV4, setTmdbUseV4] = useState(false);
  const [tmdbCredMsg, setTmdbCredMsg] = useState<string | null>(null);
  const [tmdbBusy, setTmdbBusy] = useState<'idle' | 'validating' | 'saving'>('idle');
  const [showTmdbSecrets, setShowTmdbSecrets] = useState(false);

  // Profile Management State
  const [editingProfile, setEditingProfile] = useState<Partial<UserProfile> | null>(null);
  const [isCreatingProfile, setIsCreatingProfile] = useState(false);
  const [livePaused, setLivePaused] = useState(false);
  const [showFolderPicker, setShowFolderPicker] = useState(false);
  const [showHelpModal, setShowHelpModal] = useState(false);
  const [showNoMatchModal, setShowNoMatchModal] = useState(false);
  const [showSystemLogs, setShowSystemLogs] = useState(false);
  const [logsTail, setLogsTail] = useState<{ path: string; lines: string[]; error?: string } | null>(null);
  const [health, setHealth] = useState<any | null>(null);
  const [enrichHistory, setEnrichHistory] = useState<any[]>([]);
  const [finalizeBusy, setFinalizeBusy] = useState(false);
  const [finalizeMsg, setFinalizeMsg] = useState<string | null>(null);
  const [localize, setLocalize] = useState<any | null>(null);
  const prevPending = useRef<number | null>(null);
  const prevCurrentId = useRef<number | null>(null);
  const [liveEnrichment, setLiveEnrichment] = useState<any | null>(enrichment || null);
  const pollRef = useRef<number | null>(null);
  const enrich = liveEnrichment || enrichment || null;
  const activeJob = (localize && localize.running === true)
    ? { ...(localize || {}), job: 'localize' }
    : (enrich && enrich.running === true)
      ? { ...(enrich || {}), job: 'enrich' }
      : null;

  const formatEnrichStep = (rawStep: any) => {
    const s = typeof rawStep === 'string' ? rawStep : '';
    if (!s) return t('starting');
    const [base, ...rest] = s.split(' ');
    const progress = rest.join(' ').trim();
    const label = t('normalizing_user_language');
    if (base === 'tmdb_sync' || base === 'backfill_episodes' || base === 'series_localize') {
      return progress ? `${label} (${progress})` : label;
    }
    return s;
  };

  const clampPct = (v: number) => {
    if (!Number.isFinite(v)) return 0;
    if (v < 0) return 0;
    if (v > 100) return 100;
    return v;
  };

  const parseStepProgress = (rawStep: any): { done: number; total: number } | null => {
    const s = String(rawStep || '');
    const m = s.match(/(\d+)\s*\/\s*(\d+)/);
    if (!m) return null;
    const done = Number(m[1]);
    const total = Number(m[2]);
    if (!Number.isFinite(done) || !Number.isFinite(total) || total <= 0) return null;
    return { done, total };
  };

  const computeScanProgress = () => {
    try {
      const processed = (scanStatus as any).processed ?? (scanStatus as any).processedFiles;
      const total = (scanStatus as any).total ?? (scanStatus as any).totalFiles;
      if (typeof processed === 'number' && typeof total === 'number' && total > 0) {
        return clampPct((processed / total) * 100);
      }
    } catch {}
    const p = Number(scanStatus.progress || 0);
    return clampPct(scanStatus.scanning ? Math.min(p, 99) : p);
  };

  const computeEnrichProgress = (job: any) => {
    try {
      const total = Number(job?.total ?? 0);
      const pendingTotal = (typeof job?.pending_total === 'number') ? Number(job.pending_total) : undefined;
      const pending = (typeof job?.pending === 'number') ? Number(job.pending) : undefined;
      if (total > 0 && (pendingTotal !== undefined || pending !== undefined)) {
        const left = pendingTotal !== undefined ? pendingTotal : (pending as number);
        return clampPct(((total - left) / total) * 100);
      }
    } catch {}
    const step = parseStepProgress(job?.current_step);
    if (step) return clampPct((step.done / step.total) * 100);
    return 0;
  };

  const handleAddPath = (e: React.FormEvent) => {
    e.preventDefault();
    if (newPathInput.trim()) {
      onAddPath(newPathInput);
      setNewPathInput('');
    }
  };

  // Load TMDB credentials status once
  React.useEffect(() => {
    (async () => {
      try {
        const s = await (await import('../../services/api')).MediaService.getCredentialsStatus();
        setTmdbConfigured(!!s.tmdb_configured);
      } catch (e) {
        setTmdbConfigured(false);
      }
    })();
  }, []);

  const refreshSystemInfo = async () => {
    try {
      const api = await import('../../services/api');
      const h = await api.MediaService.getHealth();
      setHealth(h || null);
    } catch {
      setHealth(null);
    }
  };

  const refreshLogs = async () => {
    try {
      const api = await import('../../services/api');
      const t = await api.MediaService.getLogsTail(250);
      setLogsTail(t || null);
    } catch {
      setLogsTail({ path: '', lines: [], error: 'failed' });
    }
  };

  // Track enrichment events and build a lightweight history/log for realtime UI
  useEffect(() => {
    // keep local liveEnrichment in sync with prop when provided
    if (!enrichment) return;
    setLiveEnrichment(prev => ({ ...(prev || {}), ...(enrichment || {}) }));
    if (livePaused) return;
    try {
      const now = Date.now();
      // detect current id changes (processing new item)
      const curId = enrichment.current_id ? Number(enrichment.current_id) : null;
      if (curId && curId !== prevCurrentId.current) {
        prevCurrentId.current = curId;
        setEnrichHistory(h => [{ type: 'processing', id: curId, title: enrichment.current_title || null, ts: now }, ...h].slice(0, 200));
      }

      // detect ingestion by seeing pending decrease
      if (typeof enrichment.pending === 'number') {
        const p = Number(enrichment.pending);
        if (prevPending.current != null && p < prevPending.current) {
          const diff = prevPending.current - p;
          setEnrichHistory(h => [{ type: 'ingested', count: diff, ts: now }, ...h].slice(0, 200));
        }
        prevPending.current = p;
      }
    } catch (e) {
      // ignore
    }
  }, [enrichment, livePaused]);

  // Poll backend enrich status when component mounts and while not paused
  useEffect(() => {
    let mounted = true;
    const startPolling = async () => {
      try {
        const api = (await import('../../services/api')).MediaService;
        const fetchOnce = async () => {
          try {
            const data = await api.getEnrichStatus();
            if (!mounted) return;
            // merge to avoid wiping unchanged fields (reduce UI flicker)
            setLiveEnrichment(prev => ({ ...(prev || {}), ...(data || {}) }));

            try {
              const loc = await api.getLocalizeStatus();
              if (mounted) {
                const normalizedLoc = loc ? { ...loc } : null;
                if (normalizedLoc && normalizedLoc.running !== true) {
                  normalizedLoc.current_title = null;
                  normalizedLoc.current_step = 'idle';
                }
                setLocalize(normalizedLoc);
              }
            } catch (e) {
              // ignore
            }

            // update history based on change in pending / current_id
            const now = Date.now();
            const curId = data.current_id ? Number(data.current_id) : null;
            if (curId && curId !== prevCurrentId.current) {
              prevCurrentId.current = curId;
              setEnrichHistory(h => [{ type: 'processing', id: curId, title: data.current_title || null, ts: now }, ...h].slice(0, 200));
            }
            if (typeof data.pending === 'number') {
              const p = Number(data.pending);
              if (prevPending.current != null && p < prevPending.current) {
                const diff = prevPending.current - p;
                setEnrichHistory(h => [{ type: 'ingested', count: diff, ts: now }, ...h].slice(0, 200));
              }
              prevPending.current = p;
            }
          } catch (e) {
            // silent
          }
        };

        // initial fetch
        await fetchOnce();
        // clear any existing interval
        if (pollRef.current) {
          window.clearInterval(pollRef.current);
          pollRef.current = null;
        }
        pollRef.current = window.setInterval(() => {
          if (livePaused) return;
          void fetchOnce();
        }, 2000) as unknown as number;
      } catch (e) {
        // ignore dynamic import errors
      }
    };

    startPolling();
    return () => {
      mounted = false;
      if (pollRef.current) {
        window.clearInterval(pollRef.current);
        pollRef.current = null;
      }
    };
  }, [livePaused]);

  const handleTestTmdb = async () => {
    setTmdbTestResult('testing');
    try {
      const r = await (await import('../../services/api')).MediaService.getCredentialsCheck();
      if (r.ok) setTmdbTestResult('ok');
      else setTmdbTestResult(r.detail || 'failed');
    } catch (e) {
      setTmdbTestResult(String(e));
    }
    // refresh configured status after test
    try {
      const s = await (await import('../../services/api')).MediaService.getCredentialsStatus();
      setTmdbConfigured(!!s.tmdb_configured);
    } catch (e) { }
  };

  const normalizeBearerToken = (raw: string) => {
    const t = (raw || '').trim();
    if (!t) return '';
    return t.toLowerCase().startsWith('bearer ') ? t.slice(7).trim() : t;
  };

  const buildTmdbPayload = () => {
    const payload: any = { tmdb_use_v4: !!tmdbUseV4 };
    const key = (tmdbApiKeyInput || '').trim();
    const token = normalizeBearerToken(tmdbAccessTokenInput || '');
    if (key) payload.tmdb_api_key = key;
    if (token) payload.tmdb_access_token = token;
    return payload;
  };

  const handleRetryNoMatch = async () => {
    try {
      const api = (await import('../../services/api')).MediaService;
      const r = await api.resetNoMatch();
      try {
        const resetCount = Number((r as any)?.data?.reset ?? 0);
        setEnrichHistory(h => [{ type: 'reset_no_match', count: resetCount, ts: Date.now() }, ...h].slice(0, 200));
      } catch (e) {
        // ignore
      }
      try {
        await api.startEnrichment();
      } catch (e) {
        // ignore
      }
      try {
        const data = await api.getEnrichStatus();
        setLiveEnrichment(prev => ({ ...(prev || {}), ...(data || {}) }));
      } catch (e) {
        // ignore
      }
    } catch (e) {
      // ignore
    }
  };

  const handleFinalizeLocalization = async () => {
    setFinalizeMsg(null);
    setFinalizeBusy(true);
    try {
      const api = (await import('../../services/api')).MediaService;
      const r = await api.startLocalization();
      if (r && r.ok) {
        setFinalizeMsg(t('process_started_ok'));
      } else {
        setFinalizeMsg((r as any)?.data?.detail || (r as any)?.error || t('process_failed'));
      }
      try {
        const data = await api.getEnrichStatus();
        setLiveEnrichment(prev => ({ ...(prev || {}), ...(data || {}) }));
      } catch (e) {
        // ignore
      }
    } catch (e) {
      setFinalizeMsg(String(e));
    } finally {
      setFinalizeBusy(false);
    }
  };

  const handleValidateTmdbCredentials = async () => {
    const payload = buildTmdbPayload();
    if (!payload.tmdb_api_key && !payload.tmdb_access_token) {
      setTmdbCredMsg(t('tmdb_enter_api_or_token'));
      return;
    }

    setTmdbBusy('validating');
    setTmdbCredMsg(null);
    try {
      const r = await (await import('../../services/api')).MediaService.checkCredentials(payload);
      if (r.ok) {
        setTmdbCredMsg(t('tmdb_creds_valid'));
        setTmdbTestResult('ok');
      } else {
        const msg = r.detail || 'failed';
        setTmdbCredMsg(`${t('tmdb_validation_failed')}: ${msg}`);
        setTmdbTestResult(msg);
      }
    } catch (e) {
      setTmdbCredMsg(`${t('tmdb_validation_failed')}: ${String(e)}`);
      setTmdbTestResult(String(e));
    } finally {
      setTmdbBusy('idle');
      try {
        const s = await (await import('../../services/api')).MediaService.getCredentialsStatus();
        setTmdbConfigured(!!s.tmdb_configured);
      } catch (e) { }
    }
  };

  const handleSaveTmdbCredentials = async () => {
    const payload = buildTmdbPayload();
    if (!payload.tmdb_api_key && !payload.tmdb_access_token && !payload.tmdb_use_v4) {
    setTmdbCredMsg(t('nothing_to_save'));
      return;
    }

    setTmdbBusy('saving');
    setTmdbCredMsg(null);
    try {
      const resp = await (await import('../../services/api')).MediaService.saveCredentials(payload);
      if (resp && resp.saved) {
        if (resp.tmdb_ok) {
          const started = resp.enrichment_started ? t('enrich_started') : t('enrich_already_running');
          setTmdbCredMsg(`${t('saved_ok')} ${t('tmdb_ok')}. ${started}`);
          setTmdbTestResult('ok');
          setTmdbApiKeyInput('');
          setTmdbAccessTokenInput('');
          setShowTmdbSecrets(false);
          setShowTmdbCredentials(false);
        } else {
          const msg = resp.detail || 'failed_check';
          setTmdbCredMsg(`${t('tmdb_saved_but_check_failed')}: ${msg}`);
          setTmdbTestResult(msg);
        }
      } else {
        setTmdbCredMsg(t('tmdb_save_failed'));
      }
    } catch (e) {
      setTmdbCredMsg(`${t('tmdb_save_failed')}: ${String(e)}`);
    } finally {
      setTmdbBusy('idle');
      try {
        const s = await (await import('../../services/api')).MediaService.getCredentialsStatus();
        setTmdbConfigured(!!s.tmdb_configured);
      } catch (e) { }
    }
  };

  const handleProfileSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!editingProfile?.name) return;

    const pinRaw = String((editingProfile as any).pin || '').replace(/\D/g, '');
    const pin = pinRaw ? pinRaw.slice(0, 6) : undefined;
    const isManager = !!(editingProfile as any).isManager && !(editingProfile as any).isKid;
    if (isManager && (!pin || pin.length < 4)) {
      window.alert(t('manager_pin_required_alert'));
      return;
    }
    if (!isManager && pin && pin.length < 4) {
      window.alert(t('pin_length_alert'));
      return;
    }

    if (isCreatingProfile) {
      onAddProfile({
        name: editingProfile.name,
        avatarColor: editingProfile.avatarColor || AVATAR_COLORS[0],
        avatarImage: (editingProfile as any).avatarImage,
        isKid: editingProfile.isKid || false,
        isManager,
        pin,
        language: editingProfile.language || 'en-US'
      });
    } else if (editingProfile.id) {
      onUpdateProfile({ ...(editingProfile as any), isManager, pin } as UserProfile);
    }

    setEditingProfile(null);
    setIsCreatingProfile(false);
  };

  const startCreateProfile = () => {
    setEditingProfile({
      name: '',
      avatarColor: AVATAR_COLORS[Math.floor(Math.random() * AVATAR_COLORS.length)],
      isKid: false,
      isManager: false,
      pin: undefined,
      language: 'en-US'
    });
    setIsCreatingProfile(true);
  };

  const startEditProfile = (profile: UserProfile) => {
    setEditingProfile({ ...profile });
    setIsCreatingProfile(false);
  };

  return (
    <div className="max-w-6xl mx-auto py-8 px-4 sm:px-8 space-y-8 pb-32 animate-fade-in">
      <div className="flex items-center justify-between mb-8">
        <h1 className="text-3xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-white to-slate-400">
          {t('settings_title')}
        </h1>
        <div className="flex items-center gap-3">
          <div className="hidden sm:flex items-center gap-2 px-3 py-1.5 rounded-lg bg-slate-800/50 border border-white/5">
            <label className="text-xs font-semibold text-slate-400">{t('app_language')}</label>
            <select
              value={lang}
              onChange={(e) => {
                const v = String(e.target.value || '').toLowerCase();
                if (v === 'es' || v === 'en') setLang(v, 'manual');
              }}
              className="bg-transparent text-sm text-slate-200 focus:outline-none cursor-pointer [&>option]:bg-[#1e293b]"
              title={t('app_language_title')}
            >
              <option value="es">{t('spanish')}</option>
              <option value="en">English</option>
            </select>
          </div>
          <Button size="sm" variant="ghost" onClick={() => setShowHelpModal(true)}>{t('help')}</Button>
          <Button size="sm" onClick={() => onRefresh && onRefresh()} icon={<RefreshCw size={14} />}>{t('refresh')}</Button>
        </div>
      </div>

      {/* Top Section: Profiles */}
      <div className="glass-panel p-6 rounded-2xl border border-white/5">
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-xl font-semibold flex items-center text-indigo-100">
            <Users className="mr-3 text-indigo-400" /> {t('profiles')}
          </h2>
          {!editingProfile && (
            <Button size="sm" onClick={startCreateProfile} icon={<Plus size={16} />}>
              {t('add_profile')}
            </Button>
          )}
        </div>

        {editingProfile ? (
          <form onSubmit={handleProfileSubmit} className="bg-slate-800/40 p-6 rounded-xl border border-white/5">
            <h3 className="text-lg font-medium text-white mb-4">
              {isCreatingProfile ? t('create_profile') : t('edit_profile')}
            </h3>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div className="space-y-4">
                <div>
                  <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider mb-2">{t('name')}</label>
                  <input
                    type="text"
                    value={editingProfile.name}
                    onChange={e => setEditingProfile({ ...editingProfile, name: e.target.value })}
                    className="w-full bg-slate-900/50 border border-white/10 rounded-lg px-4 py-2.5 text-sm text-white focus:ring-2 focus:ring-indigo-500 focus:border-transparent outline-none"
                    placeholder={t('profile_name')}
                    required
                  />
                </div>

                <div>
                  <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider mb-2">{t('language')}</label>
                  <select
                    value={editingProfile.language}
                    onChange={e => setEditingProfile({ ...editingProfile, language: e.target.value })}
                    className="w-full bg-slate-900/50 border border-white/10 rounded-lg px-4 py-2.5 text-sm text-white focus:ring-2 focus:ring-indigo-500 focus:border-transparent outline-none"
                  >
                    <option value="en-US">{t('english_us')}</option>
                    <option value="es-ES">{t('spanish_mx')}</option>
                  </select>
                </div>

                <div className="flex items-center space-x-3 pt-2">
                  <input
                    type="checkbox"
                    id="isKid"
                    checked={editingProfile.isKid}
                    onChange={e => setEditingProfile({ ...editingProfile, isKid: e.target.checked, ...(e.target.checked ? { isManager: false } : {}) })}
                    className="rounded border-slate-600 bg-slate-800 text-indigo-500 focus:ring-indigo-500"
                  />
                  <label htmlFor="isKid" className="text-sm text-slate-300">{t('kid_profile_restricted')}</label>
                </div>

                <div className="flex items-center space-x-3 pt-2">
                  <input
                    type="checkbox"
                    id="isManager"
                    checked={!!(editingProfile as any).isManager}
                    onChange={e => setEditingProfile({ ...editingProfile, isManager: e.target.checked } as any)}
                    disabled={!!(editingProfile as any).isKid}
                    className="rounded border-slate-600 bg-slate-800 text-indigo-500 focus:ring-indigo-500 disabled:opacity-50"
                  />
                  <label htmlFor="isManager" className="text-sm text-slate-300">{t('manager_profile_access')}</label>
                </div>

                <div>
                  <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider mb-2 mt-4">{t('pin_optional')}</label>
                  <input
                    type="password"
                    inputMode="numeric"
                    value={String((editingProfile as any).pin || '')}
                    onChange={e => setEditingProfile({ ...editingProfile, pin: String(e.target.value || '').replace(/\\D/g, '').slice(0, 6) } as any)}
                    className="w-full bg-slate-900/50 border border-white/10 rounded-lg px-4 py-2.5 text-sm text-white focus:ring-2 focus:ring-indigo-500 focus:border-transparent outline-none"
                    placeholder={t('pin_digits_placeholder')}
                  />
                  <p className="text-xs text-slate-500 mt-2">{t('pin_optional_desc')}</p>
                </div>
              </div>

              <div className="space-y-4">
                <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider mb-2">{t('avatar_color')}</label>
                <div className="flex flex-wrap gap-3">
                  {AVATAR_COLORS.map(color => (
                    <button
                      type="button"
                      key={color}
                      onClick={() => setEditingProfile({ ...editingProfile, avatarColor: color })}
                      className={`w-10 h-10 rounded-full transition-all ${color} ${editingProfile.avatarColor === color ? 'ring-2 ring-white scale-110' : 'opacity-70 hover:opacity-100'}`}
                    />
                  ))}
                </div>

                <div className="mt-4 flex justify-center">
                  <div className={`w-20 h-20 rounded-xl flex items-center justify-center shadow-lg overflow-hidden ${editingProfile.avatarColor}`}>
                    {(editingProfile as any).avatarImage ? (
                      <img src={(editingProfile as any).avatarImage} alt={editingProfile.name || 'avatar'} className="w-full h-full object-cover" />
                    ) : (
                      <span className="text-2xl font-bold text-white">{editingProfile.name?.[0] || '?'}</span>
                    )}
                  </div>
                </div>

                <div className="mt-4 flex items-center justify-center gap-3">
                  <label className="text-xs text-slate-400">
                    {t('photo_optional')}
                    <input
                      type="file"
                      accept="image/*"
                      onChange={async (e) => {
                        const f = e.target.files && e.target.files[0];
                        if (!f) return;
                        try {
                          const reader = new FileReader();
                          const dataUrl: string = await new Promise((resolve, reject) => {
                            reader.onerror = () => reject(new Error('read_failed'));
                            reader.onload = () => resolve(String(reader.result || ''));
                            reader.readAsDataURL(f);
                          });
                          setEditingProfile({ ...editingProfile, avatarImage: dataUrl } as any);
                        } catch (err) {
                          // ignore
                        } finally {
                          try { e.target.value = ''; } catch { }
                        }
                      }}
                      className="block mt-2 text-[11px] text-slate-300 file:mr-3 file:py-2 file:px-3 file:rounded-md file:border-0 file:bg-slate-800 file:text-slate-200 hover:file:bg-slate-700"
                    />
                  </label>
                  {(editingProfile as any).avatarImage ? (
                    <button
                      type="button"
                      onClick={() => setEditingProfile({ ...editingProfile, avatarImage: undefined } as any)}
                      className="px-3 py-2 text-[11px] text-slate-300 hover:text-white rounded-md border border-white/10 hover:bg-white/5 transition-colors self-end"
                    >
                      Quitar
                    </button>
                  ) : null}
                </div>
              </div>
            </div>

            <div className="flex justify-end space-x-3 mt-8">
              <Button type="button" variant="ghost" onClick={() => setEditingProfile(null)}>{t('cancel')}</Button>
              <Button type="submit" icon={<Save size={16} />}>{t('save_profile')}</Button>
            </div>
          </form>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
            {profiles.map(profile => (
              <div key={profile.id} className="bg-slate-800/30 p-4 rounded-xl border border-white/5 flex items-center justify-between group hover:bg-slate-800/50 transition-colors">
                <div className="flex items-center space-x-3">
                  <div className={`w-10 h-10 rounded-lg flex items-center justify-center overflow-hidden ${profile.avatarColor}`}>
                    {profile.avatarImage ? (
                      <img src={profile.avatarImage} alt={profile.name} className="w-full h-full object-cover" loading="lazy" />
                    ) : (
                      <span className="font-bold text-white">
                        {profile?.name?.[0] || (profile?.name ? profile.name.charAt(0) : '?')}
                      </span>
                    )}
                  </div>
                  <div>
                    <h4 className="font-medium text-white">{profile.name}</h4>
                    <p className="text-xs text-slate-500">{profile.isKid ? t('kids') : t('adult')} • {profile.language}</p>
                  </div>
                </div>
                <div className="flex space-x-1 opacity-0 group-hover:opacity-100 transition-opacity">
                  <button
                    onClick={() => startEditProfile(profile)}
                    className="p-1.5 hover:bg-white/10 rounded text-slate-400 hover:text-white"
                  >
                    <Pencil size={14} />
                  </button>
                  <button
                    onClick={() => onDeleteProfile(profile.id)}
                    className="p-1.5 hover:bg-red-500/10 rounded text-slate-400 hover:text-red-400"
                    disabled={profiles.length <= 1} // Prevent deleting last profile
                  >
                    <Trash2 size={14} />
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-8">

        {/* Left Column: Storage & Paths */}
        <div className="xl:col-span-2 space-y-8">

          {/* Storage Drives */}
          <div className="glass-panel p-6 rounded-2xl border border-white/5">
            <h2 className="text-xl font-semibold flex items-center mb-6 text-indigo-100">
              <HardDrive className="mr-3 text-indigo-400" /> {t('storage')}
            </h2>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {drives.map((drive, idx) => {
                const totalSpace = drive.totalSpace || 0;
                const freeSpace = drive.freeSpace || 0;
                const usedSpace = Math.max(0, totalSpace - freeSpace);
                const percentage = totalSpace > 0 ? (usedSpace / totalSpace) * 100 : 0;
                const displayPath = (drive.path || drive.label || t('unknown')).toString();

                return (
                  <div key={idx} className="bg-slate-800/40 p-5 rounded-xl border border-white/5 hover:bg-slate-800/60 transition-colors">
                    <div className="flex justify-between items-start mb-4">
                      <div className="flex items-center">
                        <div className="w-10 h-10 rounded-lg bg-gradient-to-br from-slate-700 to-slate-600 flex items-center justify-center mr-3 shadow-lg">
                          <span className="text-sm font-bold text-white">{displayPath.replace(':', '')}</span>
                        </div>
                        <div>
                          <p className="font-medium text-white">{drive.label || t('local_disk')}</p>
                          <p className="text-xs text-slate-400 font-mono">{drive.path || displayPath}</p>
                        </div>
                      </div>
                      <div className="text-right">
                        <p className="text-sm font-bold text-white">
                          {Math.round(freeSpace / 1073741824)} GB
                        </p>
                        <p className="text-[10px] uppercase tracking-wider text-slate-500">{t('free_space')}</p>
                      </div>
                    </div>

                    <div className="relative pt-1">
                      <div className="flex mb-2 items-center justify-between">
                        <div className="text-right w-full">
                          <span className="text-xs font-semibold inline-block text-indigo-300">
                            {t('free_percent').replace('{percent}', percentage.toFixed(1))}
                          </span>
                        </div>
                      </div>
                      <div className="overflow-hidden h-2 mb-4 text-xs flex rounded-full bg-slate-700/50">
                        <div
                          style={{ width: `${percentage}%` }}
                          className={`shadow-none flex flex-col text-center whitespace-nowrap text-white justify-center transition-all duration-700 ${percentage > 90 ? 'bg-gradient-to-r from-red-500 to-orange-500' : 'bg-gradient-to-r from-indigo-500 to-blue-500'}`}
                        />
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Library Paths */}
          <div className="glass-panel p-6 rounded-2xl border border-white/5">
            <h2 className="text-xl font-semibold flex items-center mb-6 text-indigo-100">
              <FolderPlus className="mr-3 text-indigo-400" /> {t('library_folders')}
            </h2>

            <form onSubmit={handleAddPath} className="flex gap-3 mb-6">
              <input
                type="text"
                value={newPathInput}
                onChange={(e) => setNewPathInput(e.target.value)}
                placeholder={t('library_path_placeholder')}
                className="flex-1 bg-slate-900/50 border border-white/10 rounded-lg px-4 py-2.5 text-sm text-white focus:ring-2 focus:ring-indigo-500 focus:border-transparent outline-none transition-all placeholder-slate-600"
              />
              <Button type="button" variant="ghost" onClick={() => setShowFolderPicker(true)} icon={<FolderPlus size={16} />}>
                {t('browse')}
              </Button>
              <Button type="submit" disabled={!newPathInput.trim()} icon={<Plus size={16} />}>
                {t('add')}
              </Button>
            </form>

            <FolderPickerModal
              open={showFolderPicker}
              onClose={() => setShowFolderPicker(false)}
              onConfirm={(paths) => {
                try {
                  for (const p of paths || []) onAddPath(p);
                } finally {
                  setShowFolderPicker(false);
                }
              }}
            />

            <div className="space-y-2">
              {libraryPaths.length === 0 ? (
                <div className="text-center py-10 border border-dashed border-slate-700 rounded-xl bg-slate-800/20">
                  <FolderPlus className="mx-auto h-10 w-10 text-slate-600 mb-3" />
                  <p className="text-slate-400 font-medium">{t('no_watch_folders')}</p>
                  <p className="text-xs text-slate-500 mt-1">{t('add_directory_to_scan')}</p>
                </div>
              ) : (
                libraryPaths.map((path, idx) => (
                  <div key={idx} className="flex items-center justify-between p-4 bg-slate-800/30 rounded-xl group hover:bg-slate-800/50 border border-transparent hover:border-white/5 transition-all">
                    <div className="flex items-center overflow-hidden">
                      <div className="w-8 h-8 rounded bg-slate-700/50 flex items-center justify-center mr-3 shrink-0 text-slate-400">
                        <FolderPlus size={16} />
                      </div>
                      <span className="text-sm text-slate-200 font-mono truncate">{path}</span>
                    </div>
                    <button
                      onClick={() => onRemovePath(path)}
                      className="text-slate-500 hover:text-red-400 hover:bg-red-500/10 p-2 rounded-lg transition-colors"
                      title={t('remove_path')}
                    >
                      <Trash2 size={16} />
                    </button>
                  </div>
                ))
              )}
            </div>
          </div>

          {/* System */}
          <div className="glass-panel p-6 rounded-2xl border border-white/5">
            <h2 className="text-xl font-semibold flex items-center mb-6 text-indigo-100">
              <Database className="mr-3 text-indigo-400" /> {t('system')}
            </h2>

            <div className="flex flex-wrap gap-3">
              <Button
                size="sm"
                variant="ghost"
                onClick={() => { void refreshSystemInfo(); }}
                icon={<RefreshCw size={14} />}
              >
                {t('test_status')}
              </Button>
              <Button
                size="sm"
                variant="ghost"
                onClick={() => { setShowSystemLogs(true); void refreshLogs(); }}
                icon={<ScanLine size={14} />}
              >
                {t('view_logs')}
              </Button>
              <Button
                size="sm"
                onClick={() => {
                  void (async () => {
                    const api = await import('../../services/api');
                    api.MediaService.downloadBackup();
                  })();
                }}
                icon={<Save size={14} />}
              >
                {t('download_backup')}
              </Button>
            </div>

            {health ? (
              <div className="mt-4 rounded-xl border border-white/10 bg-slate-900/40 p-4 text-xs text-slate-300">
                <div className="flex items-center justify-between">
                  <div className="font-semibold text-white">{t('status')}</div>
                  <div className={`px-2 py-0.5 rounded-full text-[10px] font-bold ${health.ok ? 'bg-emerald-500/15 text-emerald-300' : 'bg-red-500/15 text-red-300'}`}>
                    {health.ok ? t('ok') : t('error')}
                  </div>
                </div>
                <div className="mt-2 grid grid-cols-1 md:grid-cols-2 gap-2">
                  <div><span className="text-slate-500">DB:</span> {health.db_ok ? t('ok') : (health.db_detail || t('error'))}</div>
                  <div><span className="text-slate-500">Data:</span> <span className="font-mono">{health.data_dir}</span></div>
                </div>
              </div>
            ) : null}
          </div>
        </div>

        {/* Right Column: Scan & Metadata */}
        <div className="space-y-8">

          {/* Scan Status */}
          <div className="glass-panel p-6 rounded-2xl border border-white/5 relative overflow-hidden">
            {scanStatus.scanning && (
              <div className="absolute top-0 left-0 right-0 h-1 bg-gradient-to-r from-transparent via-indigo-500 to-transparent animate-shimmer" />
            )}

            <div className="flex items-center justify-between mb-6">
              <h2 className="text-xl font-semibold flex items-center text-indigo-100">
                <ScanLine className={`mr-3 ${scanStatus.scanning ? 'text-indigo-400 animate-pulse' : 'text-slate-400'}`} />
                {t('scanner')}
              </h2>
            </div>

            <div className="bg-slate-900/50 rounded-xl p-6 border border-white/5 mb-6">
              {/* Migration banner */}
              {migrationRunning ? (
                <div className="mb-4 p-3 rounded-md bg-purple-800/40 border border-purple-700 text-sm text-purple-200">
                  {t('migration_applying')}
                </div>
              ) : null}

              {migrationError ? (
                <div className="mb-4 p-3 rounded-md bg-red-800/40 border border-red-700 text-sm text-red-200">
                  {t('migration_error')}: {migrationError}
                </div>
              ) : null}
              <div className="flex justify-between items-end mb-3">
                <span className="text-sm font-medium text-slate-300">
                  {scanStatus.scanning ? t('processing_library') : t('system_idle')}
                </span>
                <span className="text-2xl font-bold text-white tabular-nums">{Math.round(computeScanProgress())}%</span>
              </div>

              <div className="w-full h-2 bg-slate-800 rounded-full overflow-hidden mb-4">
                <div
                  className="h-full bg-gradient-to-r from-indigo-500 to-purple-500 shadow-[0_0_10px_rgba(99,102,241,0.5)] transition-all duration-300 ease-out"
                  style={{ width: `${computeScanProgress()}%` }}
                />
              </div>

              <div className="flex items-start space-x-3 text-xs">
                {scanStatus.scanning ? (
                  <>
                    <Loader2 className="animate-spin text-indigo-400 shrink-0 mt-0.5" size={14} />
                    <div className="flex-1 overflow-hidden">
                      <p className="text-indigo-300 font-medium">{t('current_task')}:</p>
                      <p className="text-slate-400 font-mono mt-0.5 truncate" title={(scanStatus as any).current || scanStatus.currentFile}>
                        {(scanStatus as any).current || scanStatus.currentFile || t('initializing')}
                      </p>
                      {typeof (scanStatus as any).processed === 'number' && typeof (scanStatus as any).total === 'number' ? (
                        <p className="text-xs text-slate-400 mt-2">{(scanStatus as any).processed} / {(scanStatus as any).total} {t('files')}</p>
                      ) : null}
                    </div>
                  </>
                ) : (
                  <p className="flex items-center text-slate-500">
                    <CheckCircle size={14} className="mr-2 text-green-500" /> {t('last_scan_success')}
                  </p>
                )}
              </div>
            </div>

            {/* Enrichment Status */}
            <div className="bg-slate-900/30 rounded-xl p-4 border border-white/5 mb-4">
              <div className="flex items-center justify-between mb-2">
                <h3 className="text-sm font-medium text-indigo-100">{t('fetch_metadata')}</h3>
                <div className="flex items-center gap-2 flex-wrap justify-end">
                  <Button size="sm" onClick={() => onStartEnrich && onStartEnrich()} disabled={!!(enrich && enrich.running) || !!(localize && localize.running) || migrationRunning} icon={<RefreshCw size={14} />}>{t('force')}</Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => void handleFinalizeLocalization()}
                    disabled={finalizeBusy || !!(enrich && enrich.running) || !!(localize && localize.running) || migrationRunning}
                    icon={finalizeBusy ? <Loader2 className="animate-spin" size={14} /> : <Database size={14} />}
                    title={t('tmdb_localize_title')}
                  >
                    {t('failsafe_es')}
                  </Button>
                  {enrich && typeof (enrich as any).no_match === 'number' && (enrich as any).no_match > 0 ? (
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={handleRetryNoMatch}
                      disabled={!!(enrich && enrich.running) || !!(localize && localize.running) || migrationRunning}
                    >
                      {t('retry_no_match')} ({(enrich as any).no_match})
                    </Button>
                  ) : null}
                  {enrich && typeof (enrich as any).no_match === 'number' && (enrich as any).no_match > 0 ? (
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => setShowNoMatchModal(true)}
                      disabled={!!(enrich && enrich.running) || !!(localize && localize.running) || migrationRunning}
                    >
                      {t('review_no_match')} ({(enrich as any).no_match})
                    </Button>
                  ) : null}
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="text-xs text-slate-400">TMDB:</span>
                    {tmdbConfigured === null ? (
                      <span className="text-xs text-slate-500">{t('checking')}</span>
                    ) : tmdbConfigured ? (
                      <span className="text-xs text-green-400">{t('configured')}</span>
                    ) : (
                      <span className="text-xs text-yellow-400">{t('not_configured')}</span>
                    )}
                    <Button
                      size="sm"
                      onClick={handleTestTmdb}
                      className="ml-2"
                      disabled={tmdbBusy !== 'idle' || tmdbTestResult === 'testing'}
                    >
                      {tmdbTestResult === 'testing' ? t('testing') : t('test_tmdb')}
                    </Button>
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => {
                        setShowTmdbCredentials(v => !v);
                        setTmdbCredMsg(null);
                        setFinalizeMsg(null);
                      }}
                      icon={showTmdbCredentials ? <X size={14} /> : <Pencil size={14} />}
                      className="ml-1"
                    >
                      {showTmdbCredentials ? t('close') : t('set_key')}
                    </Button>
                  </div>
                </div>
              </div>

              {finalizeMsg ? (
                <div className={`mb-2 text-xs break-words ${finalizeMsg.toLowerCase().includes('ok') ? 'text-green-400' : 'text-yellow-300'}`}>
                  {finalizeMsg}
                </div>
              ) : null}

              {tmdbTestResult ? (
                <div
                  className={`text-xs mb-2 break-words ${tmdbTestResult === 'ok'
                      ? 'text-green-400'
                      : tmdbTestResult === 'testing'
                        ? 'text-slate-400'
                        : 'text-red-300'
                    }`}
                >
                  {t('tmdb_test')}: {tmdbTestResult === 'ok' ? t('ok') : tmdbTestResult === 'testing' ? t('testing') : tmdbTestResult}
                </div>
              ) : null}

              {showTmdbCredentials ? (
                  <div className="mb-4 bg-slate-900/40 border border-white/5 rounded-xl p-4">
                  <div className="flex items-center justify-between mb-3">
                    <div className="text-sm font-medium text-slate-200">{t('tmdb_credentials')}</div>
                    <label className="flex items-center gap-2 text-xs text-slate-400">
                      <input
                        type="checkbox"
                        checked={showTmdbSecrets}
                        onChange={(e) => setShowTmdbSecrets(e.target.checked)}
                        className="rounded border-slate-600 bg-slate-800 text-indigo-500 focus:ring-indigo-500"
                      />
                      {t('show')}
                    </label>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider mb-2">{t('tmdb_api_key')}</label>
                      <input
                        type={showTmdbSecrets ? 'text' : 'password'}
                        value={tmdbApiKeyInput}
                        onChange={(e) => setTmdbApiKeyInput(e.target.value)}
                        className="w-full bg-slate-900/50 border border-white/10 rounded-lg px-4 py-2.5 text-sm text-white focus:ring-2 focus:ring-indigo-500 focus:border-transparent outline-none"
                        placeholder={t('tmdb_api_key_placeholder')}
                        autoComplete="off"
                      />
                    </div>
                    <div>
                      <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider mb-2">{t('tmdb_access_token')}</label>
                      <input
                        type={showTmdbSecrets ? 'text' : 'password'}
                        value={tmdbAccessTokenInput}
                        onChange={(e) => setTmdbAccessTokenInput(e.target.value)}
                        className="w-full bg-slate-900/50 border border-white/10 rounded-lg px-4 py-2.5 text-sm text-white focus:ring-2 focus:ring-indigo-500 focus:border-transparent outline-none"
                        placeholder={t('tmdb_token_placeholder')}
                        autoComplete="off"
                      />
                    </div>
                  </div>

                  <div className="mt-4 flex items-center justify-between gap-4 flex-wrap">
                    <label className="flex items-center space-x-3 cursor-pointer group">
                      <div className={`w-10 h-6 rounded-full p-1 transition-colors ${tmdbUseV4 ? 'bg-green-500' : 'bg-slate-700'}`}>
                        <div className={`w-4 h-4 bg-white rounded-full shadow-sm transition-transform ${tmdbUseV4 ? 'translate-x-4' : ''}`} />
                      </div>
                      <input
                        type="checkbox"
                        className="hidden"
                        checked={tmdbUseV4}
                        onChange={(e) => setTmdbUseV4(e.target.checked)}
                      />
                      <span className="text-sm text-slate-300 group-hover:text-white transition-colors">{t('use_v4_token')}</span>
                    </label>

                    <div className="flex items-center gap-2">
                      <Button
                        size="sm"
                        variant="secondary"
                        onClick={handleValidateTmdbCredentials}
                        disabled={tmdbBusy !== 'idle' || (!tmdbApiKeyInput.trim() && !tmdbAccessTokenInput.trim())}
                        icon={tmdbBusy === 'validating' ? <Loader2 className="animate-spin" size={14} /> : <CheckCircle size={14} />}
                      >
                        {t('validate')}
                      </Button>
                      <Button
                        size="sm"
                        onClick={handleSaveTmdbCredentials}
                        disabled={tmdbBusy !== 'idle' || (!tmdbApiKeyInput.trim() && !tmdbAccessTokenInput.trim() && !tmdbUseV4)}
                        icon={tmdbBusy === 'saving' ? <Loader2 className="animate-spin" size={14} /> : <Save size={14} />}
                      >
                        {t('save')}
                      </Button>
                    </div>
                  </div>

                  <div className="mt-3 text-xs text-slate-500">
                    {t('saved_in')} <span className="font-mono">data/app_config.json</span> {t('on_server')}
                  </div>

                  {tmdbCredMsg ? (
                    <div className={`mt-2 text-xs break-words ${tmdbCredMsg.toLowerCase().includes('failed') || tmdbCredMsg.toLowerCase().includes('error') || tmdbCredMsg.toLowerCase().includes('fall') || tmdbCredMsg.toLowerCase().includes('no se pudo') ? 'text-red-300' : tmdbCredMsg.toLowerCase().includes('saved') || tmdbCredMsg.toLowerCase().includes('guard') || tmdbCredMsg.toLowerCase().includes('valid') || tmdbCredMsg.toLowerCase().includes('ok') ? 'text-green-400' : 'text-yellow-300'}`}>
                      {tmdbCredMsg}
                    </div>
                  ) : null}
                </div>
              ) : null}

              <div className="flex items-center justify-between mb-2">
                <div className="flex items-center gap-3">
                  <label className="text-xs text-slate-400">{t('active')}</label>
                  <button onClick={() => setLivePaused(p => !p)} className={`px-2 py-1 text-xs rounded ${livePaused ? 'bg-slate-700 text-yellow-300' : 'bg-slate-800 text-slate-200'}`}>
                    {livePaused ? t('resume') : t('pause')}
                  </button>
                </div>
                <div className="text-xs text-slate-400">{t('recent_activity')}</div>
              </div>

              {activeJob && activeJob.running ? (
                <div className="flex items-start space-x-3">
                  <Loader2 className="animate-spin text-purple-400" size={16} />
                  <div className="flex-1">
                    <p className="text-sm text-indigo-300 font-medium">{t('fetching')}: {activeJob.current_title || t('unknown')}</p>
                    <p className="text-xs text-slate-400 mt-1">{t('step')}: {formatEnrichStep(activeJob.current_step)}</p>
                    {activeJob.last_updated ? (
                      <p className="text-xs text-slate-500 mt-1">{t('updated')}: {new Date(activeJob.last_updated * 1000).toLocaleTimeString()}</p>
                    ) : null}
                    {activeJob.job === 'enrich' && typeof (activeJob as any).total === 'number' ? (
                      <div className="mt-3">
                        <div className="flex items-center justify-between text-xs text-slate-400 mb-1">
                          <span>{t('db_ingestion')}</span>
                          <span>{((activeJob as any).total - (typeof (activeJob as any).pending_total === 'number' ? (activeJob as any).pending_total : (activeJob as any).pending))} / {(activeJob as any).total}</span>
                        </div>
                        <div className="w-full h-2 bg-slate-800 rounded-full overflow-hidden">
                          <div
                            style={{
                              width: `${computeEnrichProgress(activeJob)}%`
                            }}
                            className="h-full bg-gradient-to-r from-green-400 to-indigo-500"
                          />
                        </div>
                      </div>
                    ) : null}

                    {/* Recent activity log */}
                    {activeJob.job === 'enrich' ? <div className="mt-3">
                      <div className="text-xs text-slate-400 mb-2">{t('logs')}</div>
                      <div className="max-h-48 overflow-auto bg-slate-900/40 p-2 rounded-md border border-white/5 text-xs">
                        {enrichHistory.length === 0 ? (
                          <div className="text-slate-500">{t('no_recent_events')}</div>
                        ) : (
                          enrichHistory.map((ev, i) => (
                            <div key={i} className="flex items-center justify-between py-1 border-b border-white/3 last:border-b-0">
                              <div>
                                <div className="text-slate-200">{ev.type === 'processing' ? `${t('processing')}: ${ev.title || ('#' + ev.id)}` : ev.type === 'ingested' ? `${t('ingested')} ${ev.count} ${t('items')}` : String(ev.type)}</div>
                                <div className="text-slate-400 text-[11px]">{new Date(ev.ts).toLocaleTimeString()}</div>
                              </div>
                              <div className="text-slate-400 text-[11px] ml-4">{ev.id ? `#${ev.id}` : ''}</div>
                            </div>
                          ))
                        )}
                      </div>
                    </div> : null}
                  </div>
                </div>
              ) : (
                <div>
                  <div className="text-sm text-slate-400 mb-2">{t('no_processes')}</div>
                  {enrich && typeof enrich.total === 'number' ? (
                    <div>
                      <div className="flex items-center justify-between text-xs text-slate-400 mb-1">
                        <span>{t('pending')}</span>
                        <span>{enrich.pending} {t('pending_of')} {enrich.total} {t('total')}</span>
                      </div>
                      {typeof (enrich as any).pending_total === 'number' ? (
                        <div className="flex items-center justify-between text-[11px] text-slate-500 mb-1">
                          <span>{t('unenriched')}</span>
                          <span>{(enrich as any).pending_total}</span>
                        </div>
                      ) : null}
                      {typeof (enrich as any).no_match === 'number' ? (
                        <div className="flex items-center justify-between text-[11px] text-slate-500 mb-1">
                          <span>{t('no_match')}</span>
                          <span>{(enrich as any).no_match}</span>
                        </div>
                      ) : null}
                      {typeof (enrich as any).error === 'number' ? (
                        <div className="flex items-center justify-between text-[11px] text-slate-500 mb-1">
                          <span>{t('error')}</span>
                          <span>{(enrich as any).error}</span>
                        </div>
                      ) : null}
                      <div className="w-full h-2 bg-slate-800 rounded-full overflow-hidden">
                        <div
                          style={{
                            width: `${computeEnrichProgress(enrich)}%`
                          }}
                          className="h-full bg-gradient-to-r from-green-400 to-indigo-500"
                        />
                      </div>
                    </div>
                  ) : null}
                </div>
              )}
            </div>

            <Button
              onClick={onStartScan}
              disabled={scanStatus.scanning || libraryPaths.length === 0}
              className="w-full"
              size="lg"
              icon={scanStatus.scanning ? <Loader2 className="animate-spin" size={18} /> : <RefreshCw size={18} />}
            >
              {scanStatus.scanning ? t('stop_scan') : t('start_full_scan')}
            </Button>
          </div>

          {/* Metadata Configuration */}
          <div className="glass-panel p-6 rounded-2xl border border-white/5">
            <h2 className="text-xl font-semibold flex items-center mb-6 text-indigo-100">
              <Database className="mr-3 text-indigo-400" /> {t('metadata')}
            </h2>

            <div className="space-y-6">
              {/* Movies Source */}
              <div>
                <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider mb-3">{t('movies_tv_agent')}</label>
                <div className="grid grid-cols-3 gap-2">
                  {['tmdb', 'tvdb', 'omdb'].map((provider) => (
                    <button
                      key={provider}
                      onClick={() => {
                        if (provider !== 'tmdb') return;
                        onUpdateConfig({ ...metadataConfig, moviesProvider: provider as any });
                      }}
                      disabled={provider !== 'tmdb'}
                      className={`
                        py-2 px-3 rounded-lg text-sm font-medium transition-all
                        ${metadataConfig.moviesProvider === provider
                          ? 'bg-indigo-600 text-white shadow-lg shadow-indigo-500/20'
                          : provider !== 'tmdb' ? 'bg-slate-900/40 text-slate-600 cursor-not-allowed opacity-60' : 'bg-slate-800 text-slate-400 hover:bg-slate-700 hover:text-slate-200'}
                      `}
                      title={provider !== 'tmdb' ? t('not_available_tmdb_only') : undefined}
                    >
                      {provider === 'tmdb' ? 'TMDB' : provider.toUpperCase()}
                    </button>
                  ))}
                </div>
              </div>

              {/* Anime Source */}
              <div>
                <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider mb-3">{t('anime_agent')}</label>
                <div className="grid grid-cols-3 gap-2">
                  {['jikan', 'anilist', 'kitsu'].map((provider) => (
                    <button
                      key={provider}
                      onClick={() => {
                        if (provider !== 'jikan') return;
                        onUpdateConfig({ ...metadataConfig, animeProvider: provider as any });
                      }}
                      disabled={provider !== 'jikan'}
                      className={`
                        py-2 px-3 rounded-lg text-sm font-medium transition-all
                        ${metadataConfig.animeProvider === provider
                          ? 'bg-purple-600 text-white shadow-lg shadow-purple-500/20'
                          : provider !== 'jikan' ? 'bg-slate-900/40 text-slate-600 cursor-not-allowed opacity-60' : 'bg-slate-800 text-slate-400 hover:bg-slate-700 hover:text-slate-200'}
                      `}
                      title={provider !== 'jikan' ? t('not_available_jikan_only') : undefined}
                    >
                      {provider.charAt(0).toUpperCase() + provider.slice(1)}
                    </button>
                  ))}
                </div>
              </div>

              <div className="pt-4 border-t border-white/5">
                <label className="flex items-center space-x-3 cursor-pointer group">
                  <div className={`w-10 h-6 rounded-full p-1 transition-colors ${metadataConfig.downloadImages ? 'bg-green-500' : 'bg-slate-700'}`}>
                    <div className={`w-4 h-4 bg-white rounded-full shadow-sm transition-transform ${metadataConfig.downloadImages ? 'translate-x-4' : ''}`} />
                  </div>
                  <input
                    type="checkbox"
                    className="hidden"
                    checked={metadataConfig.downloadImages}
                    onChange={(e) => onUpdateConfig({ ...metadataConfig, downloadImages: e.target.checked })}
                  />
                  <span className="text-sm text-slate-300 group-hover:text-white transition-colors">{t('auto_download_posters')}</span>
                </label>
              </div>
            </div>
          </div>

        </div>
      </div>

      <NoMatchModal
        open={showNoMatchModal}
        onClose={() => setShowNoMatchModal(false)}
        onChanged={() => {
          if (onRefresh) onRefresh();
        }}
      />

      <HelpModal
        open={showHelpModal}
        onClose={() => setShowHelpModal(false)}
      />

      {/* Logs modal */}
      {showSystemLogs ? (
        <div className="fixed inset-0 z-[90] bg-black/70 backdrop-blur-sm p-4 flex items-center justify-center" onClick={() => setShowSystemLogs(false)}>
          <div className="w-full max-w-4xl bg-slate-950/90 border border-white/10 rounded-2xl shadow-2xl overflow-hidden" onClick={(e) => e.stopPropagation()}>
            <div className="px-5 py-4 border-b border-white/10 flex items-center justify-between">
              <div className="min-w-0">
                <div className="text-white font-semibold">{t('system_logs')}</div>
                <div className="mt-1 text-xs text-slate-500 font-mono truncate">{logsTail?.path || ''}</div>
              </div>
              <div className="flex items-center gap-2">
                <Button size="sm" variant="ghost" onClick={() => void refreshLogs()} icon={<RefreshCw size={14} />}>{t('reload')}</Button>
                <button onClick={() => setShowSystemLogs(false)} className="p-2 rounded-lg hover:bg-white/10 text-slate-300 hover:text-white"><X size={18} /></button>
              </div>
            </div>
            <div className="max-h-[65vh] overflow-auto p-4">
              {logsTail?.error ? (
                <div className="text-sm text-red-300">{t('error_label')}: {logsTail.error}</div>
              ) : (
                <pre className="text-[11px] leading-relaxed font-mono text-slate-200 whitespace-pre-wrap">
                  {(logsTail?.lines || []).join('\n') || t('no_data')}
                </pre>
              )}
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
};
