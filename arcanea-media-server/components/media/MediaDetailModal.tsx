import React, { useEffect, useState } from 'react';
import { X, Play, Plus, ThumbsUp, Calendar, Clock, Film, RefreshCw, Wrench, ImageOff } from 'lucide-react';
import { MediaItem } from '../../types';
import { Button } from '../ui/Button';
import { MediaService } from '../../services/api';
import { useI18n } from '../../i18n/i18n';
import { ManualMappingModal } from './ManualMappingModal';
import { InlineLoader } from '../ui/InlineLoader';

interface MediaDetailModalProps {
  item: MediaItem;
  onClose: () => void;
  onPlay: (item: MediaItem) => void;
  isAdmin?: boolean;
  onManualMappingSaved?: (itemId: string) => void;
  profileId?: string;
  onListChanged?: () => void;
}

export const MediaDetailModal: React.FC<MediaDetailModalProps> = ({ item, onClose, onPlay, isAdmin, onManualMappingSaved, profileId, onListChanged }) => {
  const { lang, t } = useI18n();
  // Prevent scrolling on body when modal is open
  React.useEffect(() => {
    document.body.style.overflow = 'hidden';
    return () => { document.body.style.overflow = 'unset'; };
  }, []);
  React.useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const [detail, setDetail] = useState<MediaItem | null>(null);
  const [loading, setLoading] = useState(false);
  const [refreshingMeta, setRefreshingMeta] = useState(false);
  const [refreshMsg, setRefreshMsg] = useState<string | null>(null);
  const [selectedSeason, setSelectedSeason] = useState<number | null>(null);
  const [episodeRanges, setEpisodeRanges] = useState<Record<string, number>>({});
  const [dbEpisodeSeasons, setDbEpisodeSeasons] = useState<any[] | null>(null);
  const [dbEpisodeLoading, setDbEpisodeLoading] = useState(false);
  const [showManualMapping, setShowManualMapping] = useState(false);
  const [inUserList, setInUserList] = useState(false);
  const [listBusy, setListBusy] = useState(false);
  const fallbackPoster = '/images/arcanea-poster.svg';
  const fallbackBackdrop = '/images/arcanea-backdrop.svg';

  useEffect(() => {
    let mounted = true;
    const load = async () => {
      setDetail(item);
      setLoading(true);
      try {
        const id = Number(item.id);
        const d = await MediaService.getById(id);
        if (mounted) setDetail(d);
      } catch (e) {
        console.warn('Error, en cargar', e);
        if (mounted) setDetail(item);
      } finally {
        if (mounted) setLoading(false);
      }
    };
    load();
    return () => { mounted = false; };
  }, [item]);

  useEffect(() => {
    let mounted = true;
    const run = async () => {
      const seriesLike = (((detail && (detail as any).type) || item.type) === 'series') || (((detail && (detail as any).type) || item.type) === 'anime');
      if (!seriesLike) {
        setDbEpisodeSeasons(null);
        return;
      }
      setDbEpisodeLoading(true);
      try {
        const id = Number(item.id);
        const res = await MediaService.getEpisodeSeasons(id, { includeRelated: true });
        const seasons = Array.isArray(res?.seasons) ? res.seasons : [];
        if (mounted) setDbEpisodeSeasons(seasons);
        // If there are multiple seasons, default the selector to the first season.
        if (mounted && seasons.length >= 2 && selectedSeason == null) {
          const sn = Number(seasons[0]?.season_number ?? seasons[0]?.seasonNumber ?? 1);
          if (Number.isFinite(sn)) setSelectedSeason(sn);
        }
      } catch (e) {
        if (mounted) setDbEpisodeSeasons(null);
      } finally {
        if (mounted) setDbEpisodeLoading(false);
      }
    };
    run();
    return () => { mounted = false; };
  }, [item.id, item.type, detail]);

  useEffect(() => {
    let mounted = true;
    const run = async () => {
      if (!profileId) return;
      try {
        const res = await MediaService.getUserListItem(Number(item.id), profileId);
        if (mounted) setInUserList(!!res?.in_list);
      } catch {
        if (mounted) setInUserList(false);
      }
    };
    run();
    return () => { mounted = false; };
  }, [item.id, profileId]);

  const handleRefreshMetadata = async () => {
    setRefreshingMeta(true);
    setRefreshMsg(null);
    try {
      const id = Number(item.id);
      const r = await MediaService.enrichOne(id);
      if (!r.ok) {
        setRefreshMsg(r.detail || t('refresh_metadata_error'));
        return;
      }
      const d = await MediaService.getById(id);
      setDetail(d);
      if (onManualMappingSaved) onManualMappingSaved(String(id));
      setRefreshMsg(t('updated_metadata'));
    } catch (e) {
      setRefreshMsg(String(e || t('refresh_metadata_error')));
    } finally {
      setRefreshingMeta(false);
    }
  };

  const meta: any = (detail && (detail as any).rawMetadata) || (item as any).rawMetadata || null;
  const metaRaw: any = meta && meta.raw ? meta.raw : meta;

  const isSeriesLike = (((detail && (detail as any).type) || item.type) === 'series') || (((detail && (detail as any).type) || item.type) === 'anime');

  const detectSeasonFromPath = (p: string): number | null => {
    try {
      const parts = String(p || '').split(/[\\/]/).filter(Boolean);
      for (const part of parts.slice().reverse()) {
        const seg = String(part || '').trim();
        if (!seg) continue;
        // Folder-like patterns: "Temporada 2", "Season 2", "S2", "S02"
        let m = seg.match(/\b(?:temporada|season|temp(?:\.|orada)?)\s*(\d{1,2})\b/i);
        if (m && m[1]) return Number(m[1]);
        m = seg.match(/\bs(\d{1,2})\b/i);
        if (m && m[1]) return Number(m[1]);
      }
    } catch (e) {
      // ignore
    }
    return null;
  };

  const detectSxxEyy = (p: string): { season: number; episode: number } | null => {
    try {
      const s = String(p || '');
      const m = s.match(/\bS(\d{1,2})E(\d{1,3})\b/i);
      if (!m) return null;
      const season = Number(m[1]);
      const episode = Number(m[2]);
      if (!Number.isFinite(season) || !Number.isFinite(episode)) return null;
      return { season, episode };
    } catch (e) {
      return null;
    }
  };

  const guessEpisodeNumber = (fp: string): number | null => {
    const sxe = detectSxxEyy(fp);
    if (sxe?.episode && Number.isFinite(sxe.episode)) return sxe.episode;
    try {
      const bn = (String(fp || '').split(/[\\/]/).pop() || '').trim();
      const m = bn.match(/^(\d{1,3})\b/);
      if (m && m[1]) {
        const n = Number(m[1]);
        if (Number.isFinite(n)) return n;
      }
    } catch (e) {
      // ignore
    }
    return null;
  };

  const inferSeriesRootGuess = (files: any[]): string | null => {
    try {
      for (const f of files) {
        const fp = String(f?.path || '');
        if (!fp) continue;
        const parts = fp.split(/[\\/]/).filter(Boolean);
        for (let i = parts.length - 2; i >= 0; i--) {
          const seg = String(parts[i] || '').trim();
          const m = seg.match(/\b(?:temporada|season|temp(?:\.|orada)?)\s*(\d{1,2})\b/i) || seg.match(/\bs(\d{1,2})\b/i);
          if (!m || !m[1]) continue;
          const sn = Number(m[1]);
          if (!Number.isFinite(sn) || sn <= 1) continue;
          const rootParts = parts.slice(0, i);
          if (rootParts.length) return rootParts.join('\\');
          break;
        }
      }
    } catch (e) {
      // ignore
    }
    return null;
  };

  const groupFilesBySeason = (files: any[], seasonMetaMap: Map<number, any>) => {
    const dirname = (p: string): string => {
      const parts = String(p || '').split(/[\\/]/).filter(Boolean);
      parts.pop();
      return parts.join('\\');
    };

    const seriesRootGuess = inferSeriesRootGuess(files);
    const hasSeasonHints = (() => {
      try {
        for (const f of files) {
          const fp = String(f?.path || '');
          if (!fp) continue;
          if (detectSxxEyy(fp)) return true;
          if (detectSeasonFromPath(fp) != null) return true;
        }
      } catch (e) {
        // ignore
      }
      return false;
    })();
    const groups = new Map<number | null, any[]>();

    for (const f of files) {
      const fp = String(f?.path || '');
      const sxe = detectSxxEyy(fp);
      let sn: number | null = (sxe?.season ?? detectSeasonFromPath(fp)) ?? null;

      if (sn == null && seriesRootGuess) {
        const parent = dirname(fp);
        const looksLikeRootChild = parent && parent.toLowerCase() === seriesRootGuess.toLowerCase();
        const hasSeason1Meta = seasonMetaMap.has(1);
        if (looksLikeRootChild && (hasSeason1Meta || seasonMetaMap.size >= 2)) {
          sn = 1;
        }
      }

      // If TMDB metadata has seasons but filenames don't include any season hints,
      // assume these belong to Season 1 (common when S1 lives in the root folder).
      if (sn == null && !hasSeasonHints && seasonMetaMap.has(1)) {
        sn = 1;
      }

      if (!groups.has(sn)) groups.set(sn, []);
      groups.get(sn)!.push(f);
    }

    return groups;
  };

  const sortSeasonFiles = (seasonFiles: any[]): any[] => {
    return [...seasonFiles].sort((a, b) => {
      const ap = String(a?.path || '');
      const bp = String(b?.path || '');
      const ae = guessEpisodeNumber(ap);
      const be = guessEpisodeNumber(bp);
      if (ae != null && be != null) return ae - be;
      if (ae != null) return -1;
      if (be != null) return 1;
      return ap.localeCompare(bp);
    });
  };

  const buildPlaylistForSeason = (seasonFiles: any[], seasonNumber: number | null): any[] => {
    const sorted = sortSeasonFiles(seasonFiles);
    const seasonMeta = seasonNumber != null ? seasonMetaMap.get(seasonNumber) : null;
    const seasonEpisodes = (seasonMeta && Array.isArray((seasonMeta as any).episodes)) ? (seasonMeta as any).episodes : null;

    return sorted.map((f, i) => {
      const fp = String(f?.path || '');
      const ep = guessEpisodeNumber(fp) ?? (i + 1);
      const seasonEp = seasonEpisodes
        ? seasonEpisodes.find((e: any) => Number(e?.episode_number) === Number(ep)) || seasonEpisodes[ep - 1]
        : null;
      const episodeTitle = (seasonEp && (seasonEp.title || seasonEp.name)) ? String(seasonEp.title || seasonEp.name) : undefined;

      // Normalize index fields so the player label matches actual episode number.
      return { ...f, index: ep, file_index: ep, episodeTitle, seasonNumber };
    });
  };

  const buildSeasonPlaylists = (files: any[]) => {
    const groups = groupFilesBySeason(files, seasonMetaMap);
    const out: Record<number, any[]> = {};
    for (const [sn, seasonFiles] of groups.entries()) {
      if (sn == null) continue;
      out[sn] = buildPlaylistForSeason(seasonFiles, sn);
    }
    return out;
  };

  const buildSeasonPlaylistsFromDb = (seasons: any[]): Record<number, any[]> => {
    const out: Record<number, any[]> = {};
    try {
      for (const s of seasons || []) {
        const sn = Number(s?.season_number ?? s?.seasonNumber ?? 1);
        if (!Number.isFinite(sn)) continue;
        const eps = Array.isArray(s?.episodes) ? s.episodes : [];
        out[sn] = eps.map((e: any) => {
          const epNo = Number(e?.episode_number ?? e?.episodeNumber ?? 0);
          const file = e?.file || {};
          const path = String(file?.path || '');
          const filename = String(file?.filename || (path ? path.split(/[\\/]/).pop() : '') || '');
          return {
            ...file,
            path,
            filename,
            index: Number.isFinite(epNo) && epNo > 0 ? epNo : undefined,
            file_index: file?.file_index ?? file?.fileIndex ?? (Number.isFinite(epNo) && epNo > 0 ? epNo : undefined),
            episodeTitle: e?.title || undefined,
            seasonNumber: sn,
          };
        });
      }
    } catch (e) {
      // ignore
    }
    return out;
  };

  const getSeasonMetaMap = () => {
    const out = new Map<number, any>();
    try {
      const seasonsObj =
        metaRaw?.modal?.seasons ||
        metaRaw?.i18n?.[lang]?.seasons ||
        metaRaw?.i18n?.en?.seasons ||
        metaRaw?.i18n?.es?.seasons ||
        null;

      if (!seasonsObj || typeof seasonsObj !== 'object') return out;
      for (const [k, v] of Object.entries(seasonsObj)) {
        const sn = Number(k);
        if (!Number.isFinite(sn)) continue;
        out.set(sn, v);
      }
    } catch (e) {
      // ignore
    }
    return out;
  };

  const seasonMetaMap = getSeasonMetaMap();
  const seasonNumbers = Array.from(seasonMetaMap.keys()).filter(n => Number.isFinite(n)).sort((a, b) => a - b);
  const isTmdb = String(meta?.provider || metaRaw?.provider || '').toLowerCase() === 'tmdb';

  const buildPrimaryPlayItem = (): MediaItem => {
    const base = (detail || item) as any as MediaItem;
    const files = ((detail && (detail as any).files) || (item as any).files || []) as any[];
    const related = ((detail as any)?.related || (item as any)?.related || []) as any[];
    const sequel = Array.isArray(related) ? related.find((r: any) => String(r?.relation || '').toLowerCase() === 'sequel') : null;
    const seasonPlaylists = buildSeasonPlaylists(files);

    if (!Array.isArray(files) || files.length === 0) return base;

    if (isSeriesLike && isTmdb && selectedSeason != null) {
      const groups = groupFilesBySeason(files, seasonMetaMap);
      const seasonFiles = groups.get(selectedSeason) || [];
      const playlistFiles = buildPlaylistForSeason(seasonFiles, selectedSeason);
      const first = playlistFiles[0];
      const firstPath = first?.path ? String(first.path) : '';
      if (firstPath) {
        return {
          ...(base as any),
          path: firstPath,
          files: playlistFiles as any,
          nextSeries: sequel || null,
          seasonPlaylists,
          selectedSeason,
        } as any;
      }
    }

    if (base.path) return base;
    const first = files[0];
    const firstPath = first?.path ? String(first.path) : '';
    return firstPath
      ? ({
          ...(base as any),
          path: firstPath,
          files: files as any,
          nextSeries: sequel || null,
          seasonPlaylists,
          selectedSeason,
        } as any)
      : base;
  };

  useEffect(() => {
    // Default: if TMDB has multiple seasons, pick Season 1 (or first available).
    if (!isTmdb) {
      setSelectedSeason(null);
      return;
    }
    if (!seasonNumbers.length) {
      setSelectedSeason(null);
      return;
    }
    if (seasonNumbers.length >= 2) {
      setSelectedSeason((prev) => {
        if (prev && seasonNumbers.includes(prev)) return prev;
        return seasonNumbers.includes(1) ? 1 : (seasonNumbers[0] || null);
      });
      return;
    }
    // Single-season show: no selector needed.
    setSelectedSeason(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [item.id, detail?.id, isTmdb]);

  const coerceDate = (d: any): string | null => {
    if (!d) return null;
    if (typeof d === 'string') {
      const s = d.trim();
      if (!s) return null;
      // common shapes: "YYYY-MM-DD", or ISO with time
      if (s.length >= 10 && /^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
      return s;
    }
    return null;
  };

  const firstAirDate =
    coerceDate(metaRaw?.first_air_date) ||
    coerceDate(metaRaw?.release_date) ||
    coerceDate(metaRaw?.aired?.from) ||
    coerceDate(metaRaw?.aired?.prop?.from?.string) ||
    null;

  const lastAirDate =
    coerceDate(metaRaw?.last_air_date) ||
    coerceDate(metaRaw?.aired?.to) ||
    coerceDate(metaRaw?.aired?.prop?.to?.string) ||
    null;

  const airStatus: string | null = (() => {
    const s = metaRaw?.status;
    if (!s) return null;
    try {
      return String(s);
    } catch (e) {
      return null;
    }
  })();

  const primaryGenres = (detail && detail.genre) || item.genre || [];

  const localizedMeta = (() => {
    if (!metaRaw || typeof metaRaw !== 'object') return null;
    const obj = metaRaw?.i18n?.[lang] || metaRaw?.i18n?.en || metaRaw?.i18n?.es || null;
    return obj && typeof obj === 'object' ? obj : null;
  })();

  const localizedSynopsis =
    localizedMeta?.synopsis ||
    localizedMeta?.overview ||
    (detail && detail.overview) ||
    item.overview ||
    t('no_description');

  const related = (() => {
    const rel = ((detail as any)?.related || (item as any)?.related || []) as any[];
    if (!Array.isArray(rel)) return [];
    const filtered = rel.filter((r: any) => r && r.id && r.title);
    const rank = (r: any) => {
      const relName = String(r?.relation || '').toLowerCase();
      if (relName === 'prequel') return 0;
      if (relName === 'sequel') return 1;
      return 2;
    };
    return filtered.sort((a: any, b: any) => rank(a) - rank(b));
  })();

  const heroBackdrop = (detail as any)?.backdropUrl || item.backdropUrl || item.thumbnailUrl || fallbackBackdrop;
  const heroHasBackdrop = !!((detail as any)?.backdropUrl || item.backdropUrl || item.thumbnailUrl);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-2 sm:p-6 animate-fade-in">
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/70 backdrop-blur-md transition-opacity" onClick={onClose} />
      
      {/* Modal Content */}
      <div className="relative w-[95%] sm:w-full max-w-5xl mx-auto my-[10px] sm:my-0 bg-[#18181b] rounded-2xl overflow-hidden overflow-y-auto shadow-2xl border border-white/10 flex flex-col max-h-[90vh] animate-slide-up">
        
        {/* Close Button */}
        <button 
          onClick={onClose}
          className="absolute top-3 right-3 z-20 w-10 h-10 rounded-full bg-black/40 hover:bg-black/60 text-white backdrop-blur-sm border border-white/10 transition-all flex items-center justify-center"
        >
          <X size={24} />
        </button>

        {/* Hero Section */}
        <div className="relative w-full h-[220px] max-h-[250px] min-h-[200px] sm:h-[42vh] sm:max-h-[42vh] sm:min-h-[360px] overflow-hidden rounded-t-2xl bg-[#18181b]">
          <img 
            src={heroBackdrop} 
            alt={item.title}
            className="w-full h-full object-cover object-[center_20%]" 
          />
          {!heroHasBackdrop && (
            <div className="absolute inset-0 flex items-center justify-center text-slate-200/70">
              <div className="flex items-center gap-2 text-xs px-2 py-1 rounded-full bg-black/40 backdrop-blur-md border border-white/10">
                <ImageOff size={16} />
                <span>{t('no_image')}</span>
              </div>
            </div>
          )}
          {/* Gradiente mejorado para que no se vea el corte azul */}
          <div className="absolute inset-0 bg-gradient-to-t from-[#18181b]/60 via-[#18181b]/10 to-transparent" />
          <div className="absolute inset-0 bg-gradient-to-r from-[#18181b]/20 via-transparent to-transparent" />
          <div className="absolute inset-0 bg-gradient-to-b from-transparent via-black/20 to-black/80 sm:opacity-0" />
          
          <div className="absolute bottom-0 left-0 right-0 p-4 sm:p-10 z-10">
            <div className="relative max-h-[160px] sm:max-h-[200px] overflow-hidden">
              <h2 className="text-2xl sm:text-5xl font-bold text-white mb-3 sm:mb-4 drop-shadow-lg leading-tight line-clamp-2">
                {item.title}
              </h2>
              
              <div className="flex items-center flex-wrap gap-1 sm:gap-2 mb-3 sm:mb-5 text-xs sm:text-base">
                {((detail && detail.rating) || item.rating) && (
                  <span className="text-green-400 font-bold">{((detail && detail.rating) || item.rating) * 10}% {t('match')}</span>
                )}
                <span className="text-slate-300">{(detail && detail.year) || item.year}</span>
                <span className="px-1.5 py-0.5 border border-slate-500 rounded text-xs text-slate-300 uppercase">HD</span>
                <span className="text-slate-300 capitalize">{(detail && detail.type) || item.type}</span>
              </div>
            </div>
            <div className="flex flex-row flex-nowrap sm:flex-wrap items-center gap-2 sm:gap-3 mt-4 w-full overflow-x-auto pb-1 hide-scrollbar">
              <Button 
                onClick={() => onPlay(buildPrimaryPlayItem())}
                size="lg" 
                icon={<Play size={20} fill="currentColor" />}
                className="font-bold text-lg min-w-[44px] min-h-[44px] w-12 h-12 p-0 sm:w-auto sm:h-auto sm:p-4 justify-center"
              >
                <span className="sr-only">{t('play')}</span>
                <span className="hidden sm:inline">{t('play')}</span>
              </Button>
              {isAdmin ? (
                <Button
                  variant="ghost"
                  size="lg"
                  icon={<RefreshCw size={20} />}
                  onClick={handleRefreshMetadata}
                  disabled={refreshingMeta}
                  className="border border-white/20 hover:border-white min-w-[44px] min-h-[44px] w-12 h-12 p-0 sm:w-auto sm:h-auto sm:p-4 justify-center"
                >
                  <span className="sr-only">{refreshingMeta ? t('updating') : t('update_metadata')}</span>
                  <span className="hidden sm:inline">{refreshingMeta ? t('updating') : t('update_metadata')}</span>
                </Button>
              ) : null}
              {isAdmin ? (
                <Button
                  variant="ghost"
                  size="lg"
                  icon={<Wrench size={20} />}
                  onClick={() => setShowManualMapping(true)}
                  className="border border-white/20 hover:border-white min-w-[44px] min-h-[44px] w-12 h-12 p-0 sm:w-auto sm:h-auto sm:p-4 justify-center"
                >
                  <span className="sr-only">{t('fix_match')}</span>
                  <span className="hidden sm:inline">{t('fix_match')}</span>
                </Button>
              ) : null}
              <Button 
                variant={inUserList ? "primary" : "secondary"}
                size="lg" 
                icon={<Plus size={20} />}
                className="min-w-[44px] min-h-[44px] w-12 h-12 p-0 sm:w-auto sm:h-auto sm:p-4 justify-center"
                onClick={async () => {
                  if (!profileId || listBusy) return;
                  setListBusy(true);
                  try {
                    const res = await MediaService.toggleUserListItem(Number(item.id), profileId);
                    setInUserList(!!res?.in_list);
                    if (onListChanged) onListChanged();
                  } catch {
                    // ignore
                  } finally {
                    setListBusy(false);
                  }
                }}
              >
                <span className="sr-only">{inUserList ? t('in_my_list') : t('my_list')}</span>
                <span className="hidden sm:inline">{inUserList ? t('in_my_list') : t('my_list')}</span>
              </Button>
              <Button 
                variant="ghost" 
                size="lg" 
                className="rounded-full p-3 border border-white/20 hover:border-white w-full sm:w-auto justify-center"
              >
                <ThumbsUp size={20} />
              </Button>
            </div>
            {refreshMsg ? (
              <div className={`mt-3 text-xs ${refreshMsg.toLowerCase().includes('fail') || refreshMsg.toLowerCase().includes('error') ? 'text-red-300' : 'text-emerald-300'}`}>
                {refreshMsg}
              </div>
            ) : null}
          </div>
        </div>

        {/* Content Section */}
        <div className="flex-1 px-5 sm:px-12 py-4 sm:py-6">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6 sm:gap-8">
            <div className="md:col-span-2 space-y-6">
                <div className="prose prose-invert max-w-none">
                  <p className="text-[0.95rem] sm:text-base text-slate-300 leading-[1.5]">
                    {localizedSynopsis}
                  </p>
                </div>

              {/* Meta Info Grid */}
              <div className="grid grid-cols-2 sm:grid-cols-3 gap-4 border-t border-white/10 pt-6">
                <div>
                   <span className="block text-xs text-slate-500 mb-1">{t('director')}</span>
                   <span className="text-sm text-slate-200">
                     {(() => {
                       const md = (detail && (detail as any).rawMetadata) || (item as any).rawMetadata;
                       if (!md) return '—';
                       const mdRaw = (md && md.raw) ? md.raw : md;
                       const credits = md.credits || mdRaw?.credits;
                       return md.director || (credits && credits.crew && credits.crew.find((c:any)=>c.job==='Director')?.name) || '—';
                     })()}
                   </span>
                </div>
                 <div>
                   <span className="block text-xs text-slate-500 mb-1">{t('cast')}</span>
                   <span className="text-sm text-slate-200">
                     {(() => {
                       const md = (detail && (detail as any).rawMetadata) || (item as any).rawMetadata;
                       if (!md) return '—';
                       const mdRaw = (md && md.raw) ? md.raw : md;
                       const credits = md.credits || mdRaw?.credits;
                       const c = md.cast || credits?.cast || mdRaw?.cast || md.people?.cast || md.actors;
                       if (Array.isArray(c)) return c.slice(0,5).map((x:any)=>x.name || x).join(', ');
                       if (typeof c === 'string') return c;
                       return '—';
                     })()}
                   </span>
                </div>
                 <div>
                   <span className="block text-xs text-slate-500 mb-1">{t('genres')}</span>
                   <span className="text-sm text-slate-200">{((detail && detail.genre) || item.genre)?.join(', ')}</span>
                 </div>
                 <div>
                   <span className="block text-xs text-slate-500 mb-1">{t('mal_id')}</span>
                   <span className="text-sm text-slate-200">{(detail && (detail as any).malId) || (item as any).malId || '—'}</span>
                 </div>
                 <div>
                   <span className="block text-xs text-slate-500 mb-1">{t('origin')}</span>
                   <span className="text-sm text-slate-200">{(detail && (detail as any).origin) || (item as any).origin || '—'}</span>
                 </div>
                 <div>
                   <span className="block text-xs text-slate-500 mb-1">{t('animated')}</span>
                   <span className="text-sm text-slate-200">{((detail && (detail as any).isAnimated) || (item as any).isAnimated) ? t('yes') : t('no')}</span>
                 </div>
                 <div>
                   <span className="block text-xs text-slate-500 mb-1">{t('release_year')}</span>
                   <span className="text-sm text-slate-200">{(detail && (detail as any).releaseYear) || (item as any).releaseYear || (item.year || '—')}</span>
                 </div>
                  <div>
                    <span className="block text-xs text-slate-500 mb-1">{t('duration')}</span>
                    <span className="text-sm text-slate-200">
                      {(() => {
                        const v =
                          (detail && (detail as any).runtime) ??
                          (item as any).runtime ??
                          (detail && (detail as any).duration) ??
                          item.duration ??
                          null;
                        const n = v == null ? NaN : Number(v);
                        if (!Number.isFinite(n) || n <= 0) return '—';
                        return String(Math.round(n));
                      })()} min
                    </span>
                  </div>
               </div>

              {/* Episodes Section (Mockup if series) */}
                 {isSeriesLike && (
                 <div className="mt-8">
                  <h3 className="text-xl font-bold text-white mb-4">{t('episodes')}</h3>
                  {(() => {
                    const files = ((detail && (detail as any).files) || (item as any).files || []) as any[];
                    const dbSeasons = Array.isArray(dbEpisodeSeasons) ? dbEpisodeSeasons : null;
                    if (dbEpisodeLoading) {
                      return <InlineLoader label={t('loading')} />;
                    }

                    // Prefer JOINed DB seasons/episodes (do not depend on metadata blobs).
                    if (dbSeasons && dbSeasons.length) {
                      const sortedSeasons = [...dbSeasons].sort((a, b) => {
                        const an = Number(a?.season_number ?? a?.seasonNumber ?? 0);
                        const bn = Number(b?.season_number ?? b?.seasonNumber ?? 0);
                        return (Number.isFinite(an) ? an : 0) - (Number.isFinite(bn) ? bn : 0);
                      });
                      const seasonNums = sortedSeasons
                        .map(s => Number(s?.season_number ?? s?.seasonNumber ?? 1))
                        .filter(n => Number.isFinite(n)) as number[];
                      const useSeasonSelector = seasonNums.length >= 2;
                      const visible = useSeasonSelector && selectedSeason != null ? [selectedSeason] : seasonNums;
                      const playlists = buildSeasonPlaylistsFromDb(sortedSeasons);

                      return (
                        <div className="space-y-6">
                          {useSeasonSelector ? (
                            <div className="flex items-center justify-between">
                              <span className="text-sm text-slate-400">{t('season')}</span>
                              <select
                                value={selectedSeason ?? seasonNums[0]}
                                onChange={(e) => setSelectedSeason(Number(e.target.value))}
                                className="bg-white/5 border border-white/10 rounded-lg px-3 py-2 text-sm text-slate-200 focus:outline-none focus:ring-2 focus:ring-indigo-500/40"
                              >
                                {seasonNums.map((sn) => (
                                  <option key={sn} value={sn}>{`${t('season')} ${sn}`}</option>
                                ))}
                              </select>
                            </div>
                          ) : null}

                          {visible.map((sn) => {
                            const playlistFiles = playlists[sn] || [];
                            if (!playlistFiles.length) {
                              return (
                                <div key={`db-s${sn}`} className="text-sm text-slate-500">
                                  {t('no_episodes_season')}
                                </div>
                              );
                            }
                            const rangeKey = `s${sn}`;
                            const rangeSize = 50;
                            const totalRanges = Math.ceil(playlistFiles.length / rangeSize);
                            const currentStart = Math.max(0, episodeRanges[rangeKey] ?? 0);
                            const currentRangeIndex = Math.floor(currentStart / rangeSize);
                            const visibleFiles = totalRanges > 1
                              ? playlistFiles.slice(currentStart, currentStart + rangeSize)
                              : playlistFiles;

                            return (
                              <div key={`db-s${sn}`} className="space-y-2">
                                <div className="flex items-center justify-between">
                                  <h4 className="text-base font-semibold text-white">{`${t('season')} ${sn}`}</h4>
                                  <span className="text-xs text-slate-500">{playlistFiles.length} {t('files')}</span>
                                </div>

                                {totalRanges > 1 ? (
                                  <div className="flex items-center justify-between gap-3">
                                    <span className="text-xs text-slate-500">{`${currentRangeIndex * rangeSize + 1}-${Math.min((currentRangeIndex + 1) * rangeSize, playlistFiles.length)}`}</span>
                                    <div className="flex items-center gap-2">
                                      <button
                                        className="px-2 py-1 text-xs rounded-md bg-white/5 hover:bg-white/10 text-slate-200 disabled:opacity-40"
                                        disabled={currentStart <= 0}
                                        onClick={() => setEpisodeRanges((prev) => ({ ...prev, [rangeKey]: Math.max(0, currentStart - rangeSize) }))}
                                      >
                                        {t('previous')}
                                      </button>
                                      <button
                                        className="px-2 py-1 text-xs rounded-md bg-white/5 hover:bg-white/10 text-slate-200 disabled:opacity-40"
                                        disabled={currentStart + rangeSize >= playlistFiles.length}
                                        onClick={() => setEpisodeRanges((prev) => ({ ...prev, [rangeKey]: currentStart + rangeSize }))}
                                      >
                                        {t('next')}
                                      </button>
                                    </div>
                                  </div>
                                ) : null}

                              <div className="grid grid-cols-1 gap-2">
                                {visibleFiles.map((f: any, i: number) => {
                                    const fp = String(f?.path || '');
                                    const poster = (detail as any)?.posterPath || (item as any).posterPath || fallbackPoster;
                                    const epNo = f?.index ?? f?.file_index ?? (currentStart + i + 1);
                                    const episodeWord = t('episode');
                                    const title = f?.episodeTitle ? `${epNo}. ${String(f.episodeTitle)}` : `${episodeWord} ${epNo}`;
                                    return (
                                      <div key={fp || i} className="flex items-center gap-3 bg-white/5 rounded-xl p-2 sm:p-3 hover:bg-white/10 transition cursor-pointer" onClick={() => {
                                        if (!f || !f.path) return;
                                        const playItem: MediaItem = {
                                          ...(detail || item),
                                          path: f.path,
                                          files: playlistFiles as any,
                                          nextSeries: (detail as any)?.related?.find((r: any) => String(r?.relation || '').toLowerCase() === 'sequel') || null,
                                          seasonPlaylists: buildSeasonPlaylistsFromDb(sortedSeasons),
                                          selectedSeason: sn,
                                        } as any;
                                        onPlay(playItem);
                                      }}>
                                      <div className="relative w-16 h-16 rounded-lg overflow-hidden bg-slate-800 flex-shrink-0 group hidden sm:block">
                                          <img src={poster} className="w-full h-full object-cover" />
                                          <div className="absolute inset-0 bg-black/40 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity">
                                            <Play size={20} fill="white" className="text-white" />
                                          </div>
                                        </div>
                                        <div className="flex-1 min-w-0">
                                          <h4 className="text-white text-sm sm:text-base font-medium mb-0.5 line-clamp-1 sm:line-clamp-2">{title}</h4>
                                          <p className="text-[10px] sm:text-xs text-slate-400 line-clamp-1 sm:line-clamp-2">{fp}</p>
                                        </div>
                                        <div className="flex items-center gap-2">
                                          <span className="hidden sm:inline text-sm text-slate-500 mr-2">{Math.round((f?.size || 0) / 1048576)} MB</span>
                                          <Button
                                            size="sm"
                                            onClick={(e) => {
                                              e.stopPropagation();
                                              if (!f || !f.path) return;
                                              const playItem: MediaItem = {
                                                ...(detail || item),
                                                path: f.path,
                                                files: playlistFiles as any,
                                                nextSeries: (detail as any)?.related?.find((r: any) => String(r?.relation || '').toLowerCase() === 'sequel') || null,
                                                seasonPlaylists: buildSeasonPlaylistsFromDb(sortedSeasons),
                                                selectedSeason: sn,
                                              } as any;
                                              onPlay(playItem);
                                            }}
                                            icon={<Play size={14} />}
                                          >
                                          <span className="sr-only">{t('play')}</span>
                                          <span className="hidden sm:inline">{t('play')}</span>
                                          </Button>
                                        </div>
                                      </div>
                                    );
                                  })}
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      );
                    }

                    if (!Array.isArray(files) || files.length === 0) {
                      return <div className="text-sm text-slate-500">{t('no_files')}</div>;
                    }

                    const groups = groupFilesBySeason(files, seasonMetaMap);

                    const sortedKeys = Array.from(groups.keys()).sort((a, b) => {
                      if (a == null && b == null) return 0;
                      if (a == null) return 1;
                      if (b == null) return -1;
                      return a - b;
                    });

                    const useSeasonSelector = isTmdb && seasonNumbers.length >= 2;
                    const visibleKeys = useSeasonSelector && selectedSeason != null
                      ? [selectedSeason]
                      : sortedKeys;

                    return (
                      <div className="space-y-6">
                        {visibleKeys.map((sn) => {
                          const seasonFiles = groups.get(sn) || [];
                          const playlistFiles = buildPlaylistForSeason(seasonFiles, sn);
                          const sortedSeasonFiles = playlistFiles;
                          const seasonMeta = sn != null ? seasonMetaMap.get(sn) : null;
                          const seasonTitle = sn == null
                            ? (sortedKeys.length === 1 ? t('episodes') : t('unknown_season'))
                            : (seasonMeta?.title || seasonMeta?.name || `${t('season')} ${sn}`);
                          const seasonSynopsis = sn != null ? (seasonMeta?.synopsis || seasonMeta?.overview || null) : null;
                          const rangeKey = sn == null ? 'none' : `s${sn}`;
                          const rangeSize = 50;
                          const totalRanges = Math.ceil(sortedSeasonFiles.length / rangeSize);
                          const currentStart = Math.max(0, episodeRanges[rangeKey] ?? 0);
                          const currentRangeIndex = Math.floor(currentStart / rangeSize);
                          const visibleFiles = totalRanges > 1
                            ? sortedSeasonFiles.slice(currentStart, currentStart + rangeSize)
                            : sortedSeasonFiles;

                          return (
                            <div key={sn == null ? 'none' : `s${sn}`} className="space-y-2">
                              {(!useSeasonSelector && sortedKeys.length > 1) ? (
                                <div className="flex items-center justify-between">
                                  <h4 className="text-base font-semibold text-white">{seasonTitle}</h4>
                                  <span className="text-xs text-slate-500">{sortedSeasonFiles.length} {t('files')}</span>
                                </div>
                              ) : null}
                              {(useSeasonSelector && sn != null) ? (
                                <div className="flex items-center justify-between">
                                  <h4 className="text-base font-semibold text-white">{seasonTitle}</h4>
                                  <span className="text-xs text-slate-500">{sortedSeasonFiles.length} {t('files')}</span>
                                </div>
                              ) : null}
                              {seasonSynopsis ? (
                                <div className="text-xs text-slate-400 line-clamp-3">{String(seasonSynopsis)}</div>
                              ) : null}
                              {totalRanges > 1 ? (
                                <div className="flex flex-wrap gap-2 pt-1">
                                  {Array.from({ length: totalRanges }).map((_, ri) => {
                                    const start = ri * rangeSize + 1;
                                    const end = Math.min((ri + 1) * rangeSize, sortedSeasonFiles.length);
                                    const active = ri === currentRangeIndex;
                                    return (
                                      <button
                                        key={`${rangeKey}-${ri}`}
                                        onClick={() =>
                                          setEpisodeRanges((prev) => ({
                                            ...prev,
                                            [rangeKey]: ri * rangeSize,
                                          }))
                                        }
                                        className={`px-3 py-1.5 rounded-lg text-[11px] border transition-colors ${
                                          active
                                            ? 'bg-indigo-600 text-white border-indigo-500/40'
                                            : 'bg-white/5 text-slate-200 border-white/10 hover:bg-white/10'
                                        }`}
                                        title={`${start}-${end}`}
                                      >
                                        {start}-{end}
                                      </button>
                                    );
                                  })}
                                </div>
                              ) : null}
                              <div className="space-y-2">
                                {visibleFiles.map((f, i) => {
                                  const fp = String(f?.path || '');
                                  const poster = (detail && (detail as any).posterPath) || (item as any).posterPath || item.thumbnailUrl || item.backdropUrl || fallbackPoster;
                                  const baseName = fp.split(/[\\/]/).pop() || '';
                                  const sxe = detectSxxEyy(fp);
                                  const globalIndex = currentStart + i;
                                  const epNo = sxe?.episode ?? (guessEpisodeNumber(fp) ?? (globalIndex + 1));

                                  const seasonMeta = sn != null ? seasonMetaMap.get(sn) : null;
                                  const seasonEpisodes = (seasonMeta && Array.isArray((seasonMeta as any).episodes)) ? (seasonMeta as any).episodes : null;
                                  const seasonEp = seasonEpisodes ? seasonEpisodes.find((e: any) => Number(e?.episode_number) === Number(epNo)) || seasonEpisodes[epNo - 1] : null;

                                  const fallbackEp = meta && Array.isArray((meta as any).episodes) ? (meta as any).episodes[globalIndex] : null;
                                  const title =
                                    (seasonEp && (seasonEp.title || seasonEp.name)) ||
                                    (fallbackEp && (fallbackEp.title || fallbackEp.name)) ||
                                    f?.filename ||
                                    baseName ||
                                    `${t('file')} ${i + 1}`;

                                  return (
                                    <div key={fp || i} className="flex items-center p-2 sm:p-3 rounded-lg hover:bg-white/5 transition-colors cursor-pointer group">
                                      <span className="text-base sm:text-xl font-bold text-slate-600 mr-3 sm:mr-4 group-hover:text-white">{epNo}</span>
                                      <div className="w-32 h-20 rounded bg-slate-800 mr-4 overflow-hidden relative hidden sm:block">
                                        <img src={poster} className="w-full h-full object-cover" />
                                        <div className="absolute inset-0 bg-black/40 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity">
                                          <Play size={20} fill="white" className="text-white" />
                                        </div>
                                      </div>
                                      <div className="flex-1 min-w-0">
                                        <h4 className="text-white text-sm sm:text-base font-medium mb-0.5 line-clamp-1 sm:line-clamp-2">{title}</h4>
                                        <p className="text-[10px] sm:text-xs text-slate-400 line-clamp-1 sm:line-clamp-2">{fp}</p>
                                      </div>
                                      <div className="flex items-center gap-3">
                                        <span className="hidden sm:inline text-sm text-slate-500 mr-2">{Math.round((f?.size || 0) / 1048576)} MB</span>
                                        <Button
                                          size="sm"
                                          onClick={(e) => {
                                            e.stopPropagation();
                                            if (!f || !f.path) return;
                                            const playItem: MediaItem = {
                                              ...(detail || item),
                                              path: f.path,
                                              files: playlistFiles as any,
                                              nextSeries: (detail as any)?.related?.find((r: any) => String(r?.relation || '').toLowerCase() === 'sequel') || null,
                                              seasonPlaylists: buildSeasonPlaylists(files),
                                              selectedSeason: sn ?? null,
                                            } as any;
                                            onPlay(playItem);
                                          }}
                                          icon={<Play size={14} />}
                                        >
                                          <span className="sr-only">{t('play')}</span>
                                          <span className="hidden sm:inline">{t('play')}</span>
                                        </Button>
                                      </div>
                                    </div>
                                  );
                                })}
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    );
                  })()}
                </div>
              )}
            </div>

            <div className="md:col-span-1 space-y-4">
              <div className="bg-white/5 rounded-xl p-4">
                 <h4 className="text-sm font-semibold text-slate-400 uppercase tracking-wider mb-3">{t('details')}</h4>
                 <ul className="grid grid-cols-1 sm:grid-cols-2 gap-3 text-xs sm:text-sm">
                   <li className="flex justify-between border-b border-white/5 pb-2">
                     <span className="text-slate-500 flex items-center"><Calendar size={14} className="mr-2"/> {t('year')}</span>
                     <span className="text-slate-200">{(detail && detail.year) || item.year || '—'}</span>
                   </li>

                   {firstAirDate ? (
                     <li className="flex justify-between border-b border-white/5 pb-2">
                       <span className="text-slate-500 flex items-center"><Calendar size={14} className="mr-2"/>{t('aired')}</span>
                       <span className="text-slate-200">{firstAirDate}</span>
                     </li>
                   ) : null}

                   {lastAirDate ? (
                     <li className="flex justify-between border-b border-white/5 pb-2">
                       <span className="text-slate-500 flex items-center"><Calendar size={14} className="mr-2"/>{t('ended')}</span>
                       <span className="text-slate-200">{lastAirDate}</span>
                     </li>
                   ) : null}

                   {airStatus ? (
                     <li className="flex justify-between border-b border-white/5 pb-2">
                       <span className="text-slate-500 flex items-center"><Film size={14} className="mr-2"/> {t('status')}</span>
                       <span className="text-slate-200">{airStatus}</span>
                     </li>
                   ) : null}

                   {Array.isArray(primaryGenres) && primaryGenres.length ? (
                     <li className="flex justify-between border-b border-white/5 pb-2">
                       <span className="text-slate-500 flex items-center"><Film size={14} className="mr-2"/> {t('genres')}</span>
                       <span className="text-slate-200 text-right">{primaryGenres.slice(0, 3).join(', ')}</span>
                     </li>
                   ) : null}
                   

                     <li className="flex justify-between border-b border-white/5 pb-2">
                      <span className="text-slate-500 flex items-center"><Clock size={14} className="mr-2"/> {t('duration')}</span>
                      <span className="text-slate-200">
                        {(() => {
                          const v =
                            (detail && (detail as any).runtime) ??
                            (item as any).runtime ??
                            (detail && (detail as any).duration) ??
                            item.duration ??
                            null;
                          const n = v == null ? NaN : Number(v);
                          if (!Number.isFinite(n) || n <= 0) return '—';
                          return String(Math.round(n));
                        })()} min
                      </span>
                    </li>
                  </ul>

               </div>

              {/* Selector de temporada (solo TMDB) */}
              {isTmdb && seasonNumbers.length >= 2 ? (
                <div className="bg-white/5 rounded-xl p-4">
                  <div className="text-xs font-bold text-slate-500 uppercase tracking-wider mb-2">{t('seasons')}</div>
                  <div className="flex flex-wrap gap-2">
                    {seasonNumbers.map((sn) => {
                      const active = selectedSeason === sn;
                      return (
                        <button
                          key={sn}
                          onClick={() => setSelectedSeason(sn)}
                          className={`px-3 py-1.5 rounded-lg text-xs border transition-colors ${
                            active
                              ? 'bg-indigo-600 text-white border-indigo-500/40'
                              : 'bg-white/5 text-slate-200 border-white/10 hover:bg-white/10'
                          }`}
                          title={`${t('season')} ${sn}`}
                        >
                          {sn}
                        </button>
                      );
                    })}
                  </div>
                  <div className="mt-2 text-[11px] text-slate-500">
                    {t('select_season_hint')}
                  </div>
                </div>
              ) : null}

              {related.length ? (
                <div className="bg-white/5 rounded-xl p-4">
                  <h4 className="text-sm font-semibold text-slate-400 uppercase tracking-wider mb-3">{t('related')}</h4>
                  <div className="space-y-3">
                    {related.map((r: any) => (
                      <div key={r.id} className="flex items-center gap-3">
                        <div className="w-14 h-20 rounded-md overflow-hidden bg-slate-800/60 border border-white/10">
                          <img src={r.poster_url || item.thumbnailUrl || item.backdropUrl || fallbackPoster} className="w-full h-full object-cover" />
                        </div>
                        <div className="min-w-0 flex-1">
                          <div className="text-sm font-semibold text-white truncate">{r.title}</div>
                          <div className="text-[11px] text-slate-500 uppercase">{r.relation || t('related_relation')}</div>
                        </div>
                        <Button
                          size="sm"
                          onClick={async () => {
                            onClose();
                            const nextItem = { ...(detail || item), id: r.id } as any;
                            onPlay(nextItem);
                          }}
                        >
                          {t('view')}
                        </Button>
                      </div>
                    ))}
                  </div>
                </div>
              ) : null}
             </div>
          </div>
        </div>
      </div>

      {showManualMapping ? (
        <ManualMappingModal
          item={detail || item}
          onClose={() => setShowManualMapping(false)}
          onSaved={async () => {
            try {
              const id = Number(item.id);
              const d = await MediaService.getById(id);
              const bustCache = (url: string | undefined) => {
                if (!url) return url;
                const sep = url.includes('?') ? '&' : '?';
                return `${url}${sep}_t=${Date.now()}`;
              };
              if (d) {
                (d as any).posterPath = bustCache((d as any).posterPath);
                (d as any).thumbnailUrl = bustCache((d as any).thumbnailUrl);
                (d as any).backdropUrl = bustCache((d as any).backdropUrl);
              }
              setDetail(d);
              if (onManualMappingSaved) onManualMappingSaved(String(id));
            } catch {
              // ignore
            }
          }}
        />
      ) : null}
    </div>
  );
};
