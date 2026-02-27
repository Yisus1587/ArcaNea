import { MediaItem, ScanStatus, Drive } from '../types';
import { MOCK_MEDIA } from '../constants';
import { getUiLang } from '../i18n/i18n';

const API_BASE = '/api';

let adminMode = false;

export function setAdminMode(flag: boolean) {
  adminMode = !!flag;
}

function getAdminToken(): string {
  try {
    return (localStorage.getItem('arcanea_admin_token') || '').trim();
  } catch {
    return '';
  }
}

function adminHeaders(): HeadersInit | undefined {
  if (!adminMode) return undefined;
  const headers: Record<string, string> = { 'X-Arcanea-Manager': '1', 'X-Arcanea-Role': 'admin' };
  try {
    const token = getAdminToken();
    if (token) headers['X-Arcanea-Admin-Token'] = token;
  } catch {
    // ignore
  }
  return headers;
}

function adminHeadersAny(): HeadersInit | undefined {
  const token = getAdminToken();
  if (!token) return adminHeaders();
  return {
    'X-Arcanea-Manager': '1',
    'X-Arcanea-Role': 'admin',
    'X-Arcanea-Admin-Token': token,
  };
}

function profileHeaders(profileId?: string): HeadersInit | undefined {
  const pid = (profileId || '').trim();
  if (!pid) return undefined;
  return { 'X-Arcanea-Profile-Id': pid };
}

function _getDirectApiBases(): string[] {
  // IMPORTANT:
  // - On mobile, `127.0.0.1` points to the phone itself, so never use that fallback unless we're truly on localhost.
  // - Prefer same-host port 9800 for LAN usage (release mode often serves UI on the backend port too).
  try {
    if (typeof window === 'undefined') return ['http://127.0.0.1:9800/api'];
    const host = window.location.hostname;
    const proto = window.location.protocol; // http: or https:
    const isLocal = host === 'localhost' || host === '127.0.0.1';
    const bases: string[] = [];

    // Same host, backend port 9800 (use same scheme if backend is also TLS-enabled)
    if (host) bases.push(`${proto}//${host}:9800/api`);

    // Local-only fallback
    if (isLocal) bases.push(`${proto}//127.0.0.1:9800/api`);

    // de-dupe
    return Array.from(new Set(bases));
  } catch (e) {
    return ['http://127.0.0.1:9800/api'];
  }
}

export function getStatusWsUrls(): string[] {
  try {
    if (typeof window === 'undefined') return [];
    const token = getAdminToken();
    if (!token) return [];
    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const sameHost = `${proto}//${window.location.host}/api/ws/status?admin_token=${encodeURIComponent(token)}`;
    const fallbacks = _getDirectApiBases().map((b) =>
      b
        .replace(/^http:/i, 'ws:')
        .replace(/^https:/i, 'wss:')
        .replace(/\/api\/?$/i, `/api/ws/status?admin_token=${encodeURIComponent(token)}`),
    );
    return Array.from(new Set([sameHost, ...fallbacks]));
  } catch {
    return [];
  }
}

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

function _toNumber(v: any): number | undefined {
  if (v == null) return undefined;
  const n = typeof v === 'number' ? v : parseFloat(String(v));
  return Number.isFinite(n) ? n : undefined;
}

type RootObj = { path?: string | null; type?: string | null; source?: string | null };
type RootLike = string | RootObj;

export function normalizeRoots(input: unknown): string[] {
  const arr = Array.isArray(input) ? input : [];
  const out: string[] = [];
  const seen = new Set<string>();
  for (const raw of arr) {
    let path = '';
    if (typeof raw === 'string') {
      path = raw;
    } else if (raw && typeof raw === 'object') {
      const maybe = (raw as RootObj).path;
      if (typeof maybe === 'string') path = maybe;
    }
    path = (path || '').trim();
    if (!path) continue;
    if (seen.has(path)) continue;
    seen.add(path);
    out.push(path);
  }
  return out;
}

function mapMediaType(t: string | undefined): 'movie' | 'series' | 'anime' {
  const s = (t || '').toLowerCase();
  if (s.includes('anime')) return 'anime';
  if (s.includes('series') || s.includes('tv') || s.includes('show')) return 'series';
  return 'movie';
}

function mapBackendItem(it: any): MediaItem {
  const metaList = Array.isArray(it.metadata) ? it.metadata : [];
  let metaEntry = it.primary_metadata || null;
  if (!metaEntry && metaList.length) {
    metaEntry = metaList.find((m: any) => m && m.provider) || metaList[metaList.length - 1];
  }
  let metaDataObj: any = {};
  try {
    if (metaEntry) {
      if (typeof metaEntry.data === 'string') {
        metaDataObj = JSON.parse(metaEntry.data);
      } else {
        metaDataObj = metaEntry.data || {};
      }
    }
  } catch (e) {
    metaDataObj = {};
  }

  const posterUrl = (typeof it.poster_url === 'string' && it.poster_url) ? it.poster_url : `/api/media/${it.id}/poster`;
  const files = Array.isArray(it.files) ? it.files : [];
  const primaryPath = files.length ? files[0].path : (it.canonical_path || '');

  const uiLang = getUiLang();
  const i18nObj = metaDataObj?.raw?.i18n || metaDataObj?.i18n || null;
  const localized = i18nObj && typeof i18nObj === 'object' ? (i18nObj[uiLang] || i18nObj.en || i18nObj.es || null) : null;

  const rating = _toNumber(it.rating ?? metaDataObj?.score ?? metaDataObj?.rating ?? metaDataObj?.vote_average ?? metaDataObj?.rating?.value);
  const overview =
    it?.synopsis_localized ??
    localized?.overview ??
    localized?.synopsis ??
    metaDataObj?.overview ??
    metaDataObj?.synopsis ??
    metaDataObj?.description ??
    undefined;
  const release = it.release_year ?? metaDataObj?.year ?? (metaDataObj?.release_date ? String(metaDataObj.release_date).slice(0, 4) : undefined);
  const year = release ? _toNumber(release) : undefined;

  const genres = Array.isArray(it.genres)
    ? it.genres
    : (Array.isArray(localized?.genres) ? localized.genres : (Array.isArray(metaDataObj?.genres) ? metaDataObj.genres : (metaDataObj?.genres ? [metaDataObj.genres] : undefined)));

  const cast = Array.isArray(it.cast)
    ? it.cast
    : (Array.isArray(metaDataObj?.cast) ? metaDataObj.cast : undefined);

  const resolvedTitle =
    (typeof it?.title_localized === 'string' && it.title_localized.trim())
      ? it.title_localized.trim()
      : (typeof it?.title_en === 'string' && it.title_en.trim())
        ? it.title_en.trim()
        : (localized?.title || it.title || it.base_title || '');

  const manualBackdrop =
    (typeof it.backdrop_path === 'string' && it.backdrop_path.trim())
      ? it.backdrop_path
      : undefined;

  const item: MediaItem = {
    id: String(it.id ?? ''),
    title: resolvedTitle,
    path: primaryPath,
    type: mapMediaType(it.media_type),
    year: year as number | undefined,
    duration: _toNumber(metaDataObj?.duration) as number | undefined,
    // Always use a browser-accessible URL for poster in the UI
    posterPath: posterUrl,
    thumbnailUrl: posterUrl,
    backdropUrl: manualBackdrop || metaDataObj?.backdrop_path || undefined,
    overview,
    rating: rating as number | undefined,
    genre: genres,
    addedAt: it.created_at || new Date().toISOString(),
    // quick-access enriched fields
    malId: it.mal_id ?? metaDataObj?.mal_id ?? undefined,
    isAnimated: (it.is_animated ?? metaDataObj?.is_animated) ? true : false,
    origin: it.origin ?? metaDataObj?.origin ?? undefined,
    releaseYear: it.release_year ?? metaDataObj?.release_year ?? year as number | undefined,
    runtime: _toNumber(it.runtime ?? metaDataObj?.runtime) as number | undefined,
    cast,
    };

  // Attach raw metadata and files so UI can render details and episode lists
  try {
    // Keep the normalized metadata object (includes `episodes`), but preserve the provider raw payload under `.raw`.
    (item as any).rawMetadata = metaDataObj || {};
    (item as any).files = files.map((f: any) => ({ filename: f.filename || f.filename, path: f.path, size: f.size, mtime: f.mtime, index: f.index ?? f.file_index }));
  } catch (e) {
    // ignore
  }

  return item;
}

async function fetchWithFallback(input: string, init?: RequestInit) {
  // Accept either a full URL or a path that starts with `/api`
  const tryUrls: string[] = [];
  const directBases = _getDirectApiBases();
  if (/^https?:\/\//.test(input)) {
    tryUrls.push(input);
  } else if (input.startsWith('/api')) {
    tryUrls.push(input);
    for (const b of directBases) tryUrls.push(input.replace('/api', b));
  } else {
    // relative path: prepend API_BASE
    tryUrls.push(`${API_BASE}${input.startsWith('/') ? input : `/${input}`}`);
    for (const b of directBases) tryUrls.push(`${b}${input.startsWith('/') ? input : `/${input}`}`);
  }

  let lastErr: any = null;
  for (const u of tryUrls) {
    try {
      const res = await fetch(u, init as any);
      if (res.ok) return res;
      // If we got a 404 from the proxy, try the next URL
      lastErr = new Error(`HTTP ${res.status} ${res.statusText}`);
      if (res.status === 404) continue;
      // For other non-ok statuses, still return the response so callers can handle it
      return res;
    } catch (e) {
      lastErr = e;
      // try next fallback URL
      continue;
    }
  }
  throw lastErr;
}

export const MediaService = {
  async getAll(page: number = 1, limit: number = 50, query?: string, types?: string[]): Promise<{ items: MediaItem[]; total: number }> {
    const skip = Math.max(0, (page - 1) * limit);
    try {
      const queryParam = query ? `&search=${encodeURIComponent(query)}` : '';
      const typesParam = (types && types.length) ? `&types=${encodeURIComponent(types.join(','))}` : '';
      const response = await fetchWithFallback(`${API_BASE}/media?skip=${skip}&limit=${limit}${queryParam}${typesParam}`);
      if (!response.ok) throw new Error('Network response was not ok');
      const data = await response.json();
      const items = (data.items || []).map(mapBackendItem);
      return { items, total: data.total || items.length };
    } catch (error) {
      console.warn('Backend unavailable, using mock data', error);
      await delay(300);
      let items = [...MOCK_MEDIA];
      if (types && types.length) {
        const lower = new Set(types.map(t => String(t).toLowerCase()));
        items = items.filter(i => lower.has(String(i.type).toLowerCase()));
      }
      if (query) {
        const lowerQuery = query.toLowerCase();
        items = items.filter(item => item.title.toLowerCase().includes(lowerQuery) || (item.overview && item.overview.toLowerCase().includes(lowerQuery)));
      }
      const total = items.length;
      const paged = items.slice(skip, skip + limit);
      return { items: paged as MediaItem[], total };
    }
  },

  async getNoMatchItems(page: number = 1, limit: number = 50): Promise<{ items: MediaItem[]; total: number }> {
    const skip = Math.max(0, (page - 1) * limit);
    try {
      const response = await fetchWithFallback(`${API_BASE}/media?skip=${skip}&limit=${limit}&status=NO_MATCH`, {
        headers: adminHeadersAny(),
      });
      if (!response.ok) throw new Error('Failed to fetch no-match items');
      const data = await response.json();
      const items = (data.items || []).map(mapBackendItem);
      return { items, total: data.total || items.length };
    } catch (e) {
      console.warn('Failed to fetch no-match items', e);
      return { items: [], total: 0 };
    }
  },

  async searchSuggest(query: string, limit: number = 20, types?: string[]): Promise<{ items: MediaItem[]; total: number }> {
    const q = String(query || '').trim();
    if (!q) return { items: [], total: 0 };
    const typesParam = (types && types.length) ? `&types=${encodeURIComponent(types.join(','))}` : '';
    const response = await fetchWithFallback(`${API_BASE}/media/search-suggest?query=${encodeURIComponent(q)}&limit=${limit}${typesParam}`);
    if (!response.ok) throw new Error('Failed to fetch search suggestions');
    const data = await response.json();
    const items = (data.items || []).map(mapBackendItem);
    return { items, total: data.total || items.length };
  },

  async getById(itemId: number): Promise<MediaItem> {
    const response = await fetchWithFallback(`${API_BASE}/media/${itemId}`);
    if (!response.ok) throw new Error('Failed to fetch media item');
    const data = await response.json();
    return mapBackendItem(data);
  },

  async omitMediaItem(itemId: number): Promise<{ ok: boolean; detail?: string }> {
    try {
      const res = await fetchWithFallback(`${API_BASE}/media/${itemId}/omit`, {
        method: 'POST',
        headers: adminHeadersAny(),
      });
      let data: any = null;
      try { data = await res.json(); } catch { data = null; }
      if (!res.ok) return { ok: false, detail: data?.detail || `HTTP ${res.status}` };
      return { ok: true };
    } catch (e) {
      return { ok: false, detail: String(e) };
    }
  },

  async getEpisodeSeasons(itemId: number, opts?: { includeRelated?: boolean }): Promise<any> {
    const includeRelated = opts?.includeRelated !== undefined ? !!opts.includeRelated : true;
    const response = await fetchWithFallback(`${API_BASE}/media/${itemId}/episode-seasons?include_related=${includeRelated ? 'true' : 'false'}`);
    if (!response.ok) throw new Error('Failed to fetch episode seasons');
    return await response.json();
  },

  async enrichOne(itemId: number): Promise<{ ok: boolean; detail?: string }> {
    try {
      const res = await fetchWithFallback(`${API_BASE}/media/${itemId}/enrich`, { method: 'POST', headers: adminHeadersAny() });
      let data: any = null;
      try { data = await res.json(); } catch (e) { data = null; }
      if (!res.ok) return { ok: false, detail: data?.detail || `HTTP ${res.status}` };
      if (!data?.ok) return { ok: false, detail: data?.detail || 'No se pudieron actualizar los metadatos.' };
      return { ok: true };
    } catch (e) {
      return { ok: false, detail: String(e) };
    }
  },

  getPosterUrl(itemId: number): string {
    return `${API_BASE}/media/${itemId}/poster`;
  },

  getStreamUrlFromPath(path: string): string {
    return `${API_BASE}/stream?path=${encodeURIComponent(path)}`;
  },

  async startScan(): Promise<{ status: string }> {
    try {
      const response = await fetchWithFallback(`${API_BASE}/scan`, { method: 'POST', headers: adminHeadersAny() });
      let data: any = null;
      try {
        data = await response.json();
      } catch (e) {
        data = null;
      }
      return { status: data?.status || (response.ok ? 'accepted' : 'error'), detail: data?.detail || null, raw: data } as any;
    } catch (error) {
      console.warn('Scan failed, returning mock status', error);
      return { status: 'mock_failed', detail: String(error) } as any;
    }
  },

  async getScanStatus(): Promise<ScanStatus> {
    try {
      const response = await fetchWithFallback(`${API_BASE}/scan/status`, { headers: adminHeadersAny() });
      if (!response.ok) throw new Error('Failed to fetch scan status');
      const data = await response.json();
      // Ensure enrichment is present as returned by backend
      const out: any = {
        scanning: !!data.scanning || data.status === 'scanning' || false,
        progress: typeof data.progress === 'number' ? data.progress : (data.total && data.processed ? Math.round((data.processed / data.total) * 100) : 0),
        currentFile: data.current || data.currentFile || null,
        total: data.total,
        processed: data.processed,
        status: data.status,
        enrichment: data.enrichment || null,
      };
      return out as ScanStatus;
    } catch (error) {
      return { scanning: false, progress: 100 } as ScanStatus;
    }
  },

  async getEnrichStatus(): Promise<any> {
    try {
      const response = await fetchWithFallback(`${API_BASE}/enrich/status`, { headers: adminHeadersAny() });
      if (!response.ok) throw new Error('Failed to fetch enrich status');
      const data = await response.json();
      return data;
    } catch (e) {
      return { running: false, total: 0, pending: 0, enriched: 0 };
    }
  },

  async getLocalizeStatus(): Promise<any> {
    try {
      const response = await fetchWithFallback(`${API_BASE}/localize/status`);
      if (!response.ok) throw new Error('Failed to fetch localize status');
      const data = await response.json();
      return data;
    } catch (e) {
      return { running: false };
    }
  },

  async manualSearchTmdb(query: string, language: string): Promise<any> {
    const q = encodeURIComponent(query || '');
    const lang = encodeURIComponent(language || 'es-MX');
    const response = await fetchWithFallback(`${API_BASE}/manual-mapping/tmdb/search?query=${q}&language=${lang}`, {
      headers: adminHeaders(),
    });
    if (!response.ok) throw new Error('Failed to search TMDB');
    return response.json();
  },

  async manualTmdbDetails(tmdbId: string, mediaType: string, language: string): Promise<any> {
    const id = encodeURIComponent(tmdbId);
    const mt = encodeURIComponent(mediaType || 'tv');
    const lang = encodeURIComponent(language || 'es-MX');
    const response = await fetchWithFallback(`${API_BASE}/manual-mapping/tmdb/${id}/details?media_type=${mt}&language=${lang}`, {
      headers: adminHeaders(),
    });
    if (!response.ok) throw new Error('Failed to fetch TMDB details');
    return response.json();
  },

  async manualTmdbSeasons(tmdbId: string, language: string): Promise<any> {
    const id = encodeURIComponent(tmdbId);
    const lang = encodeURIComponent(language || 'es-MX');
    const response = await fetchWithFallback(`${API_BASE}/manual-mapping/tmdb/${id}/seasons?language=${lang}`, {
      headers: adminHeaders(),
    });
    if (!response.ok) throw new Error('Failed to fetch TMDB seasons');
    return response.json();
  },

  async manualTmdbSeasonDetails(tmdbId: string, seasonNumber: number, language: string): Promise<any> {
    const id = encodeURIComponent(tmdbId);
    const lang = encodeURIComponent(language || 'es-MX');
    const response = await fetchWithFallback(`${API_BASE}/manual-mapping/tmdb/${id}/season/${seasonNumber}?language=${lang}`, {
      headers: adminHeaders(),
    });
    if (!response.ok) throw new Error('Failed to fetch TMDB season');
    return response.json();
  },

  async manualApplyMapping(itemId: number, payload: any): Promise<any> {
    const response = await fetchWithFallback(`${API_BASE}/manual-mapping/${itemId}/apply`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...(adminHeaders() || {}) },
      body: JSON.stringify(payload || {}),
    });
    if (!response.ok) throw new Error('Failed to apply manual mapping');
    return response.json();
  },

  async getRecommendations(limit: number = 20, profileId?: string): Promise<any> {
    const pid = profileId ? `&profile_id=${encodeURIComponent(profileId)}` : '';
    const response = await fetchWithFallback(`${API_BASE}/media/recommendations?limit=${encodeURIComponent(String(limit))}${pid}`, {
      headers: profileHeaders(profileId),
    });
    if (!response.ok) throw new Error('Failed to fetch recommendations');
    return response.json();
  },

  async recordPlay(itemId: number, profileId?: string): Promise<any> {
    const pid = profileId ? `?profile_id=${encodeURIComponent(profileId)}` : '';
    const response = await fetchWithFallback(`${API_BASE}/media/${itemId}/play${pid}`, {
      method: 'POST',
      headers: profileHeaders(profileId),
    });
    if (!response.ok) throw new Error('Failed to record play');
    return response.json();
  },

  async getUserList(profileId: string): Promise<any> {
    const response = await fetchWithFallback(`${API_BASE}/user-list`, {
      headers: profileHeaders(profileId),
    });
    if (!response.ok) throw new Error('Failed to fetch user list');
    return response.json();
  },

  async getUserListItem(itemId: number, profileId: string): Promise<any> {
    const response = await fetchWithFallback(`${API_BASE}/user-list/${itemId}`, {
      headers: profileHeaders(profileId),
    });
    if (!response.ok) throw new Error('Failed to fetch user list item');
    return response.json();
  },

  async toggleUserListItem(itemId: number, profileId: string): Promise<any> {
    const response = await fetchWithFallback(`${API_BASE}/user-list/${itemId}/toggle`, {
      method: 'POST',
      headers: profileHeaders(profileId),
    });
    if (!response.ok) throw new Error('Failed to toggle user list');
    return response.json();
  },

  async startLocalization(limitSeries?: number, limitSeasons?: number): Promise<any> {
    try {
      const qs: string[] = [];
      if (typeof limitSeries === 'number') qs.push(`limit_series=${encodeURIComponent(String(limitSeries))}`);
      if (typeof limitSeasons === 'number') qs.push(`limit_seasons=${encodeURIComponent(String(limitSeasons))}`);
      const url = qs.length ? `${API_BASE}/localize/start?${qs.join('&')}` : `${API_BASE}/localize/start`;
      const res = await fetchWithFallback(url, { method: 'POST', headers: adminHeaders() });
      let data = null;
      try { data = await res.json(); } catch (e) { data = null; }
      return { ok: res.ok, data };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  },

  async stopLocalization(): Promise<any> {
    try {
      const res = await fetchWithFallback(`${API_BASE}/localize/stop`, { method: 'POST', headers: adminHeaders() });
      let data = null;
      try { data = await res.json(); } catch (e) { data = null; }
      return { ok: res.ok, data };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  },

  async startEnrichment(): Promise<any> {
    try {
      const res = await fetchWithFallback(`${API_BASE}/enrich/start`, { method: 'POST', headers: adminHeadersAny() });
      let data = null;
      try { data = await res.json(); } catch (e) { data = null; }
      return { ok: res.ok, data };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  },

  async backfillEpisodes(limitSeasons?: number): Promise<any> {
    try {
      const url = typeof limitSeasons === 'number'
        ? `${API_BASE}/enrich/backfill-episodes?limit_seasons=${encodeURIComponent(String(limitSeasons))}`
        : `${API_BASE}/enrich/backfill-episodes`;
      const res = await fetchWithFallback(url, { method: 'POST', headers: adminHeadersAny() });
      let data = null;
      try { data = await res.json(); } catch (e) { data = null; }
      return { ok: res.ok, data };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  },

  async resetNoMatch(): Promise<any> {
    try {
      const res = await fetchWithFallback(`${API_BASE}/enrich/reset-no-match`, { method: 'POST', headers: adminHeadersAny() });
      let data = null;
      try { data = await res.json(); } catch (e) { data = null; }
      return { ok: res.ok, data };
    } catch (e) {
      return { ok: false, error: String(e) };
    }
  },

  async getDrives(): Promise<Drive[]> {
    try {
      const response = await fetchWithFallback(`${API_BASE}/drives`, { headers: adminHeadersAny() });
      if (!response.ok) throw new Error('Failed to fetch drives');
      const data = await response.json();
      const drivesData: any[] = data.drives || [];
      return drivesData.map(d => ({ path: d.path, label: d.label || (d.path || '').split(/[\\/]/).filter(Boolean).pop() || d.path, totalSpace: d.totalSpace || 0, freeSpace: d.freeSpace || 0 }));
    } catch (error) {
      console.warn('Failed to fetch drives, using fallback', error);
      return [
        { path: 'C:\\', label: 'C', totalSpace: 0, freeSpace: 0 },
        { path: 'D:\\', label: 'D', totalSpace: 0, freeSpace: 0 },
      ];
    }
  },
  async getRoots(): Promise<string[]> {
    try {
      const res = await fetchWithFallback(`${API_BASE}/roots`, { headers: adminHeadersAny() });
      if (!res.ok) throw new Error('Failed to fetch roots');
      const data = await res.json();
      return normalizeRoots(data.roots);
    } catch (e) {
      console.warn('Failed to fetch roots', e);
      return [];
    }
  },

  async addRoot(path: string): Promise<string[]> {
    try {
      const res = await fetchWithFallback(`${API_BASE}/roots`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...(adminHeadersAny() || {}) },
        body: JSON.stringify({ path }),
      });
      if (!res.ok) throw new Error('Failed to add root');
      const data = await res.json();
      return normalizeRoots(data.roots);
    } catch (e) {
      console.warn('Failed to add root', e);
      return [];
    }
  },

  async removeRoot(path: string): Promise<string[]> {
    try {
      const res = await fetchWithFallback(`${API_BASE}/roots`, {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json', ...(adminHeadersAny() || {}) },
        body: JSON.stringify({ path }),
      });
      if (!res.ok) throw new Error('Failed to remove root');
      const data = await res.json();
      return normalizeRoots(data.roots);
    } catch (e) {
      console.warn('Failed to remove root', e);
      return [];
    }
  },
  async getAppConfig(): Promise<any> {
    try {
      const res = await fetchWithFallback(`${API_BASE}/app-config`);
      if (!res.ok) {
        // Si es 404, el endpoint no existe, retornar objeto vacío
        if (res.status === 404) {
          console.warn('app-config endpoint not found (404), returning empty config');
          return {
            setupComplete: false,
            profiles: [],
            metadata: {},
            media_roots: []
          };
        }
        throw new Error(`HTTP ${res.status}: ${res.statusText}`);
      }
      const data = await res.json();
      console.log('App config loaded:', data);
      return data;
    } catch (e) {
      console.warn('Failed to fetch app config, using default:', e);
      // Retornar configuración por defecto
      return {
        setupComplete: false,
        profiles: [],
        metadata: {
          moviesProvider: 'tmdb',
          animeProvider: 'jikan',
          language: 'en-US',
          downloadImages: true,
          fetchCast: true
        },
        media_roots: []
      };
    }
  },

  async saveAppConfig(cfg: any): Promise<any> {
    try {
      const res = await fetchWithFallback(`${API_BASE}/app-config`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(cfg),
      });

      if (!res.ok) {
        if (res.status === 404) {
          console.warn('app-config endpoint not found (404), config not saved');
          return cfg; // Retornar la configuración como si se hubiera guardado
        }
        throw new Error(`HTTP ${res.status}: ${res.statusText}`);
      }

      return await res.json();
    } catch (e) {
      console.warn('Failed to save app config:', e);
      // En desarrollo, podemos "simular" que se guardó
      return cfg;
    }
  },
  
  async saveCredentials(payload: any): Promise<any> {
    try {
      const res = await fetchWithFallback(`${API_BASE}/credentials`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...(adminHeadersAny() || {}) },
        body: JSON.stringify(payload),
      });
      if (!res.ok) throw new Error('Failed to save credentials');
      return await res.json();
    } catch (e) {
      console.warn('Failed to save credentials', e);
      return { saved: false };
    }
  },

  async adminLogin(pin: string): Promise<{ ok: boolean; token?: string; expires_in?: number; detail?: string }> {
    try {
      const res = await fetchWithFallback(`${API_BASE}/admin/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ pin }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) return { ok: false, detail: data?.detail || 'invalid' };
      const token = String(data?.token || '');
      if (token) {
        try { localStorage.setItem('arcanea_admin_token', token); } catch { /* ignore */ }
      }
      return { ok: true, token, expires_in: data?.expires_in };
    } catch (e) {
      return { ok: false, detail: String(e) };
    }
  },

  async adminLogout(): Promise<{ ok: boolean }> {
    const token = getAdminToken();
    try {
      if (token) {
        await fetchWithFallback(`${API_BASE}/admin/logout`, {
          method: 'POST',
          headers: { 'X-Arcanea-Admin-Token': token },
        });
      }
    } catch {
      // ignore
    }
    try { localStorage.removeItem('arcanea_admin_token'); } catch { /* ignore */ }
    return { ok: true };
  },

  async getCredentialsStatus(): Promise<{ tmdb_configured: boolean }> {
    try {
      const res = await fetchWithFallback(`${API_BASE}/credentials`, { headers: adminHeadersAny() });
      if (!res.ok) throw new Error('Failed to fetch credentials status');
      const data = await res.json();
      return data;
    } catch (e) {
      return { tmdb_configured: false };
    }
  },
  async getCredentialsCheck(): Promise<{ ok: boolean; detail?: string }> {
    try {
      const res = await fetchWithFallback(`${API_BASE}/credentials/check`, { headers: adminHeadersAny() });
      if (!res.ok) throw new Error('Failed to check credentials');
      const data = await res.json();
      return data;
    } catch (e) {
      return { ok: false, detail: String(e) };
    }
  },
  async checkCredentials(payload: any): Promise<{ ok: boolean; detail?: string }> {
    try {
      const res = await fetchWithFallback(`${API_BASE}/credentials/check`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...(adminHeadersAny() || {}) },
        body: JSON.stringify(payload),
      });
      if (!res.ok) throw new Error('Failed to validate credentials');
      const data = await res.json();
      return data;
    } catch (e) {
      return { ok: false, detail: String(e) };
    }
  },
  async pickRoots(multiple: boolean = false): Promise<string[]> {
    try {
      const res = await fetchWithFallback(`${API_BASE}/roots/pick`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...(adminHeadersAny() || {}) },
        body: JSON.stringify({ multiple }),
      });
      if (!res.ok) throw new Error('Failed to pick roots');
      const data = await res.json();
      return normalizeRoots(data.paths);
    } catch (e) {
      console.warn('pickRoots failed', e);
      return [];
    }
  },

  async listDirectories(path?: string | null): Promise<{ path: string | null; parent: string | null; entries: Array<{ name: string; path: string; type: string }> }> {
    try {
      const qp = path ? `?path=${encodeURIComponent(path)}` : '';
      const res = await fetchWithFallback(`${API_BASE}/fs/list${qp}`, { headers: adminHeadersAny() });
      if (!res.ok) throw new Error(`Failed to list directories (${res.status})`);
      const data = await res.json();
      return {
        path: (typeof data?.path === 'string' ? data.path : null),
        parent: (typeof data?.parent === 'string' ? data.parent : null),
        entries: Array.isArray(data?.entries) ? data.entries : [],
      };
    } catch (e) {
      console.warn('listDirectories failed', e);
      return { path: path ?? null, parent: null, entries: [] };
    }
  },

  async getHealth(): Promise<any> {
    try {
      const res = await fetchWithFallback(`${API_BASE}/health`);
      if (!res.ok) throw new Error('Failed to fetch health');
      return await res.json();
    } catch (e) {
      return { ok: false, detail: String(e) };
    }
  },

  async getLogsInfo(): Promise<{ path: string; exists: boolean; size: number }> {
    try {
      const res = await fetchWithFallback(`${API_BASE}/logs/info`, { headers: adminHeaders() });
      if (!res.ok) throw new Error('Failed to fetch logs info');
      const data = await res.json();
      return {
        path: String(data?.path || ''),
        exists: !!data?.exists,
        size: Number(data?.size || 0),
      };
    } catch (e) {
      return { path: '', exists: false, size: 0 };
    }
  },

  async getLogsTail(lines: number = 200): Promise<{ path: string; lines: string[]; error?: string }> {
    try {
      const res = await fetchWithFallback(`${API_BASE}/logs/tail?lines=${encodeURIComponent(String(lines))}`, { headers: adminHeaders() });
      if (!res.ok) throw new Error('Failed to tail logs');
      const data = await res.json();
      return {
        path: String(data?.path || ''),
        lines: Array.isArray(data?.lines) ? data.lines.map((x: any) => String(x)) : [],
        error: data?.error ? String(data.error) : undefined,
      };
    } catch (e) {
      return { path: '', lines: [], error: String(e) };
    }
  },

  async downloadBackup(): Promise<void> {
    try {
      const res = await fetchWithFallback(`${API_BASE}/backup/export`, { headers: adminHeaders() });
      if (!res.ok) throw new Error('Failed to export backup');
      const blob = await res.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      const cd = res.headers.get('Content-Disposition') || '';
      const match = /filename=\"?([^\";]+)\"?/i.exec(cd);
      a.href = url;
      a.download = match?.[1] || 'arcanea-backup.zip';
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);
    } catch (e) {
      console.warn('Failed to export backup', e);
    }
  },
};
