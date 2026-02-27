import React, { useEffect, useMemo, useState } from 'react';
import { X, Search, Check, Loader2 } from 'lucide-react';
import { MediaItem } from '../../types';
import { Button } from '../ui/Button';
import { MediaService } from '../../services/api';
import { useI18n } from '../../i18n/i18n';

interface ManualMappingModalProps {
  item: MediaItem;
  onClose: () => void;
  onSaved?: () => void;
}

type SearchResult = {
  tmdb_id: string;
  media_type: 'tv' | 'movie';
  title: string;
  year?: string;
  poster?: string | null;
};

export const ManualMappingModal: React.FC<ManualMappingModalProps> = ({ item, onClose, onSaved }) => {
  const { lang, t } = useI18n();
  const languageOptions = [
    { value: 'es-MX', label: 'Español (MX)' },
    { value: 'es-ES', label: 'Español (ES)' },
    { value: 'es-AR', label: 'Español (AR)' },
    { value: 'pt-BR', label: 'Portugués (BR)' },
    { value: 'en-US', label: 'Inglés (US)' },
  ];
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);
  const defaultLang = useMemo(() => {
    const l = String(lang || '').toLowerCase();
    if (l.startsWith('es')) return 'es-MX';
    if (l.startsWith('pt')) return 'pt-BR';
    return 'en-US';
  }, [lang]);

  const [query, setQuery] = useState(item.title || '');
  const [language, setLanguage] = useState(defaultLang);
  const [results, setResults] = useState<SearchResult[]>([]);
  const [selected, setSelected] = useState<SearchResult | null>(null);
  const [seasons, setSeasons] = useState<{ season_number: number; name?: string }[]>([]);
  const [seasonNumber, setSeasonNumber] = useState<number | null>(null);
  const [seasonTitle, setSeasonTitle] = useState<string>('');
  const [title, setTitle] = useState<string>('');
  const [overview, setOverview] = useState<string>('');
  const [genres, setGenres] = useState<string[]>([]);
  const [episodes, setEpisodes] = useState<{ episode_number: number; title: string; overview?: string; keep?: boolean }[]>([]);
  const [posters, setPosters] = useState<string[]>([]);
  const [backdrops, setBackdrops] = useState<string[]>([]);
  const [posterSelected, setPosterSelected] = useState<string | null>(null);
  const [backdropSelected, setBackdropSelected] = useState<string | null>(null);
  const [expandedImage, setExpandedImage] = useState<string | null>(null);
  const [posterLimit, setPosterLimit] = useState(12);
  const [backdropLimit, setBackdropLimit] = useState(6);
  const [busy, setBusy] = useState(false);
  const [seasonLoading, setSeasonLoading] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [downloadAssets, setDownloadAssets] = useState(false);
  const [extraLanguages, setExtraLanguages] = useState<string[]>([]);

  const displayEpisodeMap = useMemo(() => {
    const kept = episodes.filter(e => e.keep !== false);
    const ordered = kept.sort((a, b) => (a.episode_number || 0) - (b.episode_number || 0));
    const map = new Map<number, number>();
    ordered.forEach((e, idx) => {
      if (typeof e.episode_number === 'number') map.set(e.episode_number, idx + 1);
    });
    return map;
  }, [episodes]);

  useEffect(() => {
    setLanguage(defaultLang);
  }, [defaultLang]);

  useEffect(() => {
    setExtraLanguages((prev) => prev.filter((l) => l !== language));
  }, [language]);

  const handleSearch = async () => {
    setBusy(true);
    setMessage(null);
    try {
      const res = await MediaService.manualSearchTmdb(query, language);
      const list = Array.isArray(res?.results) ? res.results : [];
      setResults(list);
    } catch (e: any) {
      setMessage(String(e?.message || e));
      setResults([]);
    } finally {
      setBusy(false);
    }
  };

  const loadDetails = async (r: SearchResult) => {
    setBusy(true);
    setMessage(null);
    try {
      const detail = await MediaService.manualTmdbDetails(r.tmdb_id, r.media_type, language);
      setTitle(detail?.title || r.title || '');
      setOverview(detail?.overview || '');
      setGenres(Array.isArray(detail?.genres) ? detail.genres : []);
      const posters = Array.isArray(detail?.posters) ? detail.posters : [];
      const backdrops = Array.isArray(detail?.backdrops) ? detail.backdrops : [];
      const posterPick = posters[0] || detail?.poster || r.poster || null;
      const backdropPick = backdrops[0] || detail?.backdrop || null;
      setPosters(posters.length ? posters : (detail?.poster ? [detail.poster] : []));
      setBackdrops(backdrops.length ? backdrops : (detail?.backdrop ? [detail.backdrop] : []));
      setPosterSelected(posterPick);
      setBackdropSelected(backdropPick);
      setPosterLimit(12);
      setBackdropLimit(6);
      if (r.media_type === 'tv') {
        const s = await MediaService.manualTmdbSeasons(r.tmdb_id, language);
        setSeasons(Array.isArray(s?.seasons) ? s.seasons : []);
      } else {
        setSeasons([]);
        setSeasonNumber(null);
        setEpisodes([]);
      }
    } catch (e: any) {
      setMessage(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  };

  const loadSeason = async (sn: number) => {
    if (!selected) return;
    setSeasonLoading(true);
    setMessage(null);
    try {
      const d = await MediaService.manualTmdbSeasonDetails(selected.tmdb_id, sn, language);
      const eps = Array.isArray(d?.episodes) ? d.episodes : [];
      setSeasonTitle(d?.title || '');
      setOverview(d?.overview || '');
      setEpisodes(
        eps.map((e: any) => ({
          episode_number: Number(e?.episode_number || 0),
          title: e?.title || '',
          overview: e?.overview || '',
          keep: true,
        }))
      );
      const p = Array.isArray(d?.posters) ? d.posters : [];
      const b = Array.isArray(d?.backdrops) ? d.backdrops : [];
      const mergedPosters = Array.from(new Set([...(posters || []), ...p]));
      const mergedBackdrops = Array.from(new Set([...(backdrops || []), ...b]));
      setPosters(mergedPosters);
      setBackdrops(mergedBackdrops);
      if (!posterSelected && mergedPosters.length) setPosterSelected(mergedPosters[0]);
      if (!backdropSelected && mergedBackdrops.length) setBackdropSelected(mergedBackdrops[0]);
      setPosterLimit(12);
      setBackdropLimit(6);
    } catch (e: any) {
      setMessage(String(e?.message || e));
    } finally {
      setSeasonLoading(false);
    }
  };

  const handleApply = async () => {
    if (!selected) return;
    setBusy(true);
    setMessage(null);
    try {
      const payload = {
        tmdb_id: selected.tmdb_id,
        media_type: selected.media_type,
        season_number: seasonNumber,
        language,
        title: title || seasonTitle || selected.title,
        overview,
        genres,
        poster_url: posterSelected,
        backdrop_url: backdropSelected,
        season_title: seasonTitle || title || '',
        download_assets: downloadAssets,
        episode_overrides: (() => {
          const kept = episodes.filter(e => e.keep !== false);
          const ordered = kept.sort((a, b) => (a.episode_number || 0) - (b.episode_number || 0));
          return ordered.map((e, idx) => ({
            episode_number: idx + 1,
            original_episode_number: e.episode_number,
            title: e.title,
            overview: e.overview,
          }));
        })(),
      };
      const res = await MediaService.manualApplyMapping(Number(item.id), payload);
      if (!res?.ok) throw new Error(res?.detail || 'save_failed');
      const extras = extraLanguages.filter((l) => l && l !== language);
      for (const lang of extras) {
        try {
          const detail = await MediaService.manualTmdbDetails(selected.tmdb_id, selected.media_type, lang);
          await MediaService.manualApplyMapping(Number(item.id), {
            tmdb_id: selected.tmdb_id,
            media_type: selected.media_type,
            season_number: seasonNumber,
            language: lang,
            title: detail?.title || selected.title,
            overview: detail?.overview || '',
            genres: Array.isArray(detail?.genres) ? detail.genres : [],
            poster_url: null,
            backdrop_url: null,
            season_title: null,
            download_assets: false,
            episode_overrides: [],
            translation_only: true,
          });
        } catch {
          // Best-effort for extra languages; keep primary save.
        }
      }
      setMessage(t('saved'));
      if (onSaved) onSaved();
    } catch (e: any) {
      setMessage(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 sm:p-6 animate-fade-in">
      <div className="absolute inset-0 bg-black/70 backdrop-blur-md" onClick={onClose} />
      <div className="relative w-full max-w-5xl bg-[#18181b] rounded-2xl overflow-hidden shadow-2xl border border-white/10 flex flex-col max-h-[90vh]">
        <button
          onClick={onClose}
          className="absolute top-4 right-4 z-20 p-2 rounded-full bg-black/40 hover:bg-black/60 text-white backdrop-blur-sm border border-white/10 transition-all"
        >
          <X size={22} />
        </button>

        <div className="px-6 pt-6 pb-4 border-b border-white/10">
          <h3 className="text-xl text-white font-semibold">{t('manual_mapping_title')}</h3>
          <p className="text-sm text-slate-400">{t('manual_mapping_subtitle')}</p>
        </div>

        <div className="flex-1 overflow-y-auto px-6 py-5 space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="md:col-span-2 space-y-3">
              <div className="flex gap-2">
                <input
                  className="flex-1 bg-black/30 border border-white/10 rounded-lg px-3 py-2 text-white"
                  placeholder={t('manual_mapping_search_placeholder')}
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                      e.preventDefault();
                      if (!busy) void handleSearch();
                    }
                  }}
                />
                <Button icon={<Search size={16} />} onClick={handleSearch} disabled={busy}>
                  {t('manual_mapping_search')}
                </Button>
              </div>
              <div className="flex items-center gap-2 text-sm text-slate-300">
                <span>{t('language')}:</span>
                <select
                  className="bg-black/30 border border-white/10 rounded-lg px-2 py-1 text-white"
                  value={language}
                  onChange={(e) => setLanguage(e.target.value)}
                >
                  {languageOptions.map((opt) => (
                    <option key={opt.value} value={opt.value}>{opt.label}</option>
                  ))}
                </select>
                <label className="ml-3 inline-flex items-center gap-2 text-xs text-slate-300">
                  <input
                    type="checkbox"
                    className="accent-indigo-500"
                    checked={downloadAssets}
                    onChange={(e) => setDownloadAssets(e.target.checked)}
                  />
                  {t('download_assets_local')}
                </label>
              </div>
              <div className="flex flex-wrap items-center gap-2 text-xs text-slate-300">
                <span className="text-slate-400">{t('save_also_in')}</span>
                {languageOptions.map((opt) => (
                  <label key={opt.value} className="inline-flex items-center gap-1 bg-white/5 border border-white/10 rounded-full px-2 py-1">
                    <input
                      type="checkbox"
                      className="accent-indigo-500"
                      checked={extraLanguages.includes(opt.value)}
                      disabled={opt.value === language}
                      onChange={(e) => {
                        const checked = e.target.checked;
                        setExtraLanguages((prev) => {
                          if (opt.value === language) return prev;
                          return checked
                            ? Array.from(new Set([...prev, opt.value]))
                            : prev.filter((l) => l !== opt.value);
                        });
                      }}
                    />
                    <span>{opt.label}</span>
                  </label>
                ))}
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                {(selected ? results.filter(r => r.tmdb_id === selected.tmdb_id && r.media_type === selected.media_type) : results).map((r) => (
                  <button
                    key={`${r.media_type}-${r.tmdb_id}`}
                    onClick={() => {
                      setSelected(r);
                      setSeasonNumber(null);
                      setEpisodes([]);
                      loadDetails(r);
                    }}
                    className={`w-full flex items-center gap-3 p-3 rounded-2xl border transition ${
                      selected?.tmdb_id === r.tmdb_id
                        ? 'border-indigo-500/50 bg-white/10 shadow-[0_0_20px_rgba(79,70,229,0.3)]'
                        : 'border-white/10 bg-white/5'
                    }`}
                  >
                    <div className="w-12 h-16 rounded-lg overflow-hidden border border-white/10 bg-white/5 shrink-0">
                      <img src={r.poster || ''} className="w-full h-full object-cover" />
                    </div>
                    <div className="text-left min-w-0">
                      <div className="text-white font-semibold truncate">{r.title}</div>
                      <div className="text-xs text-slate-400">{r.media_type.toUpperCase()} · {r.year || '—'}</div>
                    </div>
                  </button>
                ))}
              </div>
              {selected ? (
                <div className="flex justify-end">
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => setSelected(null)}
                    disabled={busy}
                  >
                    {t('change_search')}
                  </Button>
                </div>
              ) : null}
            </div>

            <div className="space-y-3">
              <div className="text-sm text-slate-400">{t('seasons')}</div>
              <select
                className="w-full bg-black/30 border border-white/10 rounded-lg px-3 py-2 text-white"
                value={seasonNumber ?? ''}
                onChange={(e) => {
                  const sn = Number(e.target.value);
                  setSeasonNumber(Number.isFinite(sn) ? sn : null);
                  if (Number.isFinite(sn)) loadSeason(sn);
                }}
                disabled={!selected || selected.media_type !== 'tv'}
              >
                <option value="">{t('select_season')}</option>
                {seasons.map((s) => (
                  <option key={s.season_number} value={s.season_number}>{`S${s.season_number} ${s.name || ''}`}</option>
                ))}
              </select>
              {seasonLoading ? (
                <div className="flex items-center gap-2 text-xs text-slate-400">
                  <Loader2 size={14} className="animate-spin" />
                  {t('loading_episodes_images')}
                </div>
              ) : null}

              <div className="text-sm text-slate-400">{t('title')}</div>
              <input
                className="w-full bg-black/30 border border-white/10 rounded-lg px-3 py-2 text-white"
                value={title}
                onChange={(e) => setTitle(e.target.value)}
              />

              <div className="text-sm text-slate-400">{t('synopsis')}</div>
              <textarea
                className="w-full bg-black/30 border border-white/10 rounded-lg px-3 py-2 text-white min-h-[120px]"
                value={overview}
                onChange={(e) => setOverview(e.target.value)}
              />
            </div>
          </div>

          {episodes.length > 0 ? (
            <div className="space-y-2">
              <div className="text-sm text-slate-400">{t('episodes')}</div>
              {seasonNumber != null ? (
                <div className="flex items-center justify-between">
                  <div className="text-xs text-slate-500">{t('episode_remove_hint')}</div>
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => setEpisodes([])}
                    disabled={busy}
                  >
                    {t('clear_episodes')}
                  </Button>
                </div>
              ) : null}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                {episodes.filter(ep => ep.keep !== false).map((ep) => (
                  <div key={ep.episode_number} className="p-3 rounded-xl border border-white/10 bg-white/5">
                    <div className="text-xs text-slate-400">
                      E{displayEpisodeMap.get(ep.episode_number) ?? ep.episode_number}
                    </div>
                    <label className="flex items-center gap-2 text-xs text-slate-400 mt-2">
                      <input
                        type="checkbox"
                        checked={ep.keep !== false}
                        onChange={(e) => {
                          const keep = e.target.checked;
                          setEpisodes(prev => prev.map(p => p.episode_number === ep.episode_number ? { ...p, keep } : p));
                        }}
                      />
                      {t('include_in_season')}
                    </label>
                    <input
                      className="w-full bg-black/30 border border-white/10 rounded-lg px-2 py-1 text-white text-sm"
                      value={ep.title}
                      onChange={(e) => {
                        setEpisodes(prev => prev.map(p => p.episode_number === ep.episode_number ? { ...p, title: e.target.value } : p));
                      }}
                    />
                    <div className="flex justify-end mt-2">
                      <Button
                        size="sm"
                        variant="ghost"
                        onClick={() => setEpisodes(prev => prev.filter(p => p.episode_number !== ep.episode_number))}
                      >
                        {t('remove')}
                      </Button>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ) : null}

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <div className="text-sm text-slate-400 mb-2">{t('posters')}</div>
              <div className="grid grid-cols-4 gap-2">
                {posters.slice(0, posterLimit).map((p) => (
                  <button key={p} onClick={() => setPosterSelected(p)} className="relative rounded-lg border border-white/10 bg-white/5">
                    <img src={p} className="w-full h-28 object-cover rounded-lg" />
                    {posterSelected === p ? (
                      <div className="absolute inset-0 bg-emerald-400/20 rounded-lg border border-emerald-400/60 flex items-center justify-center">
                        <Check size={16} className="text-emerald-200" />
                      </div>
                    ) : null}
                    <div
                      onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        setExpandedImage(p);
                      }}
                      className="absolute top-2 right-2 text-[10px] px-2 py-1 rounded bg-black/60 text-white"
                    >
                      {t('view')}
                    </div>
                  </button>
                ))}
              </div>
              {posters.length > posterLimit ? (
                <div className="mt-2 flex justify-end">
                  <Button size="sm" variant="ghost" onClick={() => setPosterLimit((p) => p + 12)}>
                    {t('show_more')}
                  </Button>
                </div>
              ) : null}
            </div>
            <div>
              <div className="text-sm text-slate-400 mb-2">{t('backdrops')}</div>
              <div className="grid grid-cols-2 gap-2">
                {backdrops.slice(0, backdropLimit).map((b) => (
                  <button key={b} onClick={() => setBackdropSelected(b)} className="relative rounded-lg border border-white/10 bg-white/5">
                    <img src={b} className="w-full h-24 object-cover rounded-lg" />
                    {backdropSelected === b ? (
                      <div className="absolute inset-0 bg-emerald-400/20 rounded-lg border border-emerald-400/60 flex items-center justify-center">
                        <Check size={16} className="text-emerald-200" />
                      </div>
                    ) : null}
                    <div
                      onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        setExpandedImage(b);
                      }}
                      className="absolute top-2 right-2 text-[10px] px-2 py-1 rounded bg-black/60 text-white"
                    >
                      {t('view')}
                    </div>
                  </button>
                ))}
              </div>
              {backdrops.length > backdropLimit ? (
                <div className="mt-2 flex justify-end">
                  <Button size="sm" variant="ghost" onClick={() => setBackdropLimit((b) => b + 6)}>
                    {t('show_more')}
                  </Button>
                </div>
              ) : null}
            </div>
          </div>

          {message ? (
            <div className="text-sm text-emerald-300">{message}</div>
          ) : null}
        </div>

        <div className="px-6 py-4 border-t border-white/10 flex justify-end gap-2">
          <Button variant="ghost" onClick={onClose}>{t('close')}</Button>
          <Button onClick={handleApply} disabled={busy || !selected}>
            {t('save_changes')}
          </Button>
        </div>
      </div>

      {expandedImage ? (
        <div className="fixed inset-0 z-[60] flex items-center justify-center p-6">
          <div className="absolute inset-0 bg-black/80" onClick={() => setExpandedImage(null)} />
          <div className="relative max-w-4xl w-full">
            <img src={expandedImage} className="w-full max-h-[80vh] object-contain rounded-xl border border-white/10" />
            <button
              onClick={() => setExpandedImage(null)}
              className="absolute top-3 right-3 bg-black/70 text-white rounded-full p-2"
            >
              <X size={18} />
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );
};
