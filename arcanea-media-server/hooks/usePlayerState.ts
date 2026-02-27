import { useCallback, useEffect, useMemo, useState } from 'react';
import { MediaItem, UserProfile } from '../types';
import { MediaService } from '../services/api';

interface UsePlayerStateParams {
  currentUser: UserProfile | null;
  lang: string;
  recordPlay: (id: string | number | null | undefined) => void;
  onCloseDetail?: () => void;
}

export const usePlayerState = ({
  currentUser,
  lang,
  recordPlay,
  onCloseDetail,
}: UsePlayerStateParams) => {
  const [playingMedia, setPlayingMedia] = useState<MediaItem | null>(null);
  const [playingSrc, setPlayingSrc] = useState<string | null>(null);
  const [playerSeasons, setPlayerSeasons] = useState<
    Array<{ key: string; label: string; episodes: any[]; baseItem: any }>
  >([]);

  const extractNextSeries = (srcItem: any): any | null => {
    try {
      const rel = srcItem?.related || srcItem?.nextSeries || null;
      if (Array.isArray(rel)) {
        return rel.find((r: any) => String(r?.relation || '').toLowerCase() === 'sequel') || null;
      }
      if (rel && typeof rel === 'object') return rel;
    } catch {
      return null;
    }
    return null;
  };

  const basenameFromPath = (p: string) => {
    try {
      const parts = String(p || '').split(/[\\/]/).filter(Boolean);
      return parts.length ? parts[parts.length - 1] : String(p || '');
    } catch (e) {
      return String(p || '');
    }
  };

  const guessEpisodeNumber = (fp: string): number | null => {
    try {
      const s = String(fp || '');
      const m = s.match(/\bS(\d{1,2})E(\d{1,3})\b/i);
      if (m) return Number(m[2]);
    } catch {}
    try {
      const bn = (String(fp || '').split(/[\\/]/).pop() || '').trim();
      const m = bn.match(/^(\d{1,3})\b/);
      if (m && m[1]) return Number(m[1]);
    } catch {}
    return null;
  };

  const buildEpisodesFromMedia = (media: any) => {
    const files = Array.isArray(media?.files) ? media.files : [];

    const sorted = [...files].sort((a, b) => {
      const ae = guessEpisodeNumber(a?.path || '') ?? a?.index ?? a?.file_index ?? 0;
      const be = guessEpisodeNumber(b?.path || '') ?? b?.index ?? b?.file_index ?? 0;
      return ae - be;
    });

    return sorted.map((f, i) => {
      const epNo = guessEpisodeNumber(f?.path || '') ?? f?.index ?? f?.file_index ?? i + 1;
      return { ...f, index: epNo, file_index: epNo, episodeTitle: f?.episodeTitle || undefined };
    });
  };

  const buildPlayerSeasons = async (base: any) => {
    if (!base) return [];
    const rel = Array.isArray(base.related) ? base.related : [];
    const prequels = rel.filter((r: any) => String(r?.relation || '').toLowerCase() === 'prequel');
    const sequels = rel.filter((r: any) => String(r?.relation || '').toLowerCase() === 'sequel');

    const sections: Array<{ key: string; label: string; episodes: any[]; baseItem: any }> = [];

    const episodesFromDb = async (media: any) => {
      try {
        const id = Number(media?.id);
        if (!Number.isFinite(id)) return buildEpisodesFromMedia(media);
        const res = await MediaService.getEpisodeSeasons(id, { includeRelated: false });
        const seasons = Array.isArray(res?.seasons) ? res.seasons : [];
        const self = seasons.find((s: any) => Number(s?.media_item_id) === id) || seasons[0] || null;
        const eps = self && Array.isArray(self.episodes) ? self.episodes : [];
        return eps.map((e: any) => {
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
            seasonNumber: Number(self?.season_number ?? self?.seasonNumber ?? undefined),
          };
        });
      } catch {
        return buildEpisodesFromMedia(media);
      }
    };

    const addSection = async (media: any, label: string) => {
      const episodes = await episodesFromDb(media);
      sections.push({ key: `m${media?.id}`, label, episodes, baseItem: media });
    };

    for (const r of prequels) {
      try {
        const d = await MediaService.getById(Number(r.id));
        await addSection(d, r.title || d.title || 'Precuela');
      } catch {}
    }

    const primaryLabel = base.selectedSeason ? `Temporada ${base.selectedSeason}` : base.title || 'Temporada';
    await addSection(base, primaryLabel);

    for (const r of sequels) {
      try {
        const d = await MediaService.getById(Number(r.id));
        await addSection(d, r.title || d.title || 'Secuela');
      } catch {}
    }

    return sections;
  };

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      if (!playingMedia) {
        setPlayerSeasons([]);
        return;
      }
      const seasons = await buildPlayerSeasons(playingMedia);
      if (!cancelled) setPlayerSeasons(seasons);
    };
    void run();
    return () => {
      cancelled = true;
    };
  }, [playingMedia?.id, playingMedia?.path, lang]);

  const handlePlayById = useCallback(
    async (id: number) => {
      try {
        const d = await MediaService.getById(id);
        const files = Array.isArray((d as any).files) ? (d as any).files : [];
        let path = (d as any).path;
        if (!path && files.length) path = files[0]?.path;
        const nextSeries = extractNextSeries(d);
        const playItem: any = { ...(d as any), path, files, nextSeries };
        onCloseDetail?.();
        setPlayingMedia(playItem);
        recordPlay(id);
        try {
          await MediaService.recordPlay(id, currentUser?.id);
        } catch {}
        if (path) {
          setPlayingSrc(MediaService.getStreamUrlFromPath(path));
        } else {
          setPlayingSrc(null);
        }
      } catch (e) {
        console.error('Failed to load media by id', e);
      }
    },
    [currentUser?.id, onCloseDetail, recordPlay],
  );

  const handlePlay = useCallback(
    (item: MediaItem) => {
      (async () => {
        onCloseDetail?.();
        const anyItem: any = item as any;
        if (!anyItem.path && anyItem.id) {
          await handlePlayById(Number(anyItem.id));
          return;
        }
        if (anyItem.seasonPlaylists && anyItem.selectedSeason) {
          const list = anyItem.seasonPlaylists[anyItem.selectedSeason] || [];
          if (Array.isArray(list) && list.length) {
            anyItem.files = list;
            anyItem.path = anyItem.path || list[0]?.path;
          }
        }
        const nextSeries = extractNextSeries(anyItem);
        setPlayingMedia({ ...(anyItem as any), nextSeries });
        recordPlay(anyItem?.id ?? null);
        try {
          if (anyItem?.id) await MediaService.recordPlay(Number(anyItem.id), currentUser?.id);
        } catch {}
        try {
          const src = MediaService.getStreamUrlFromPath(anyItem.path);
          setPlayingSrc(src);
        } catch (e) {
          console.error('Failed to get stream url', e);
          setPlayingSrc(null);
        }
      })();
    },
    [currentUser?.id, handlePlayById, onCloseDetail, recordPlay],
  );

  const playingFiles: any[] = useMemo(() => {
    if (!playingMedia) return [];
    const fs = (playingMedia as any).files;
    return Array.isArray(fs) ? fs : [];
  }, [playingMedia]);

  const playingFileIndex = useMemo(() => {
    if (!playingMedia) return -1;
    if (!playingFiles.length) return -1;
    const curPath = playingMedia.path;
    const idx = playingFiles.findIndex((f: any) => f && f.path && f.path === curPath);
    return idx >= 0 ? idx : 0;
  }, [playingFiles, playingMedia]);

  const nowPlayingLabel = useMemo(() => {
    if (!playingMedia) return '';
    if (!playingFiles.length) return basenameFromPath(playingMedia.path);
    const f = playingFiles[Math.max(0, playingFileIndex)] || null;
    const name =
      (f && (f.episodeTitle || f.title || f.name || f.filename || (f.path ? basenameFromPath(f.path) : ''))) ||
      basenameFromPath(playingMedia.path);
    const ep = (f && (typeof f.index === 'number' ? f.index : (typeof f.file_index === 'number' ? f.file_index : undefined))) as
      | number
      | undefined;
    const season = f && typeof f.seasonNumber === 'number' && Number.isFinite(f.seasonNumber) ? f.seasonNumber : undefined;
    if (typeof ep === 'number' && Number.isFinite(ep)) {
      if (typeof season === 'number') return `Temporada ${season} - Episodio ${ep}: ${name}`;
      return `Episodio ${ep}: ${name}`;
    }
    return name;
  }, [playingMedia, playingFiles, playingFileIndex]);

  const playFromPlaylistIndex = useCallback(
    (nextIdx: number) => {
      if (!playingMedia) return;
      const f = playingFiles[nextIdx];
      const nextPath = f && f.path ? String(f.path) : '';
      if (!nextPath) return;
      try {
        const src = MediaService.getStreamUrlFromPath(nextPath);
        setPlayingSrc(src);
      } catch (e) {
        console.error('Failed to build stream url', e);
      }
      setPlayingMedia({ ...playingMedia, path: nextPath });
    },
    [playingFiles, playingMedia],
  );

  const handleNextPlayingItem = useCallback(() => {
    if (!playingMedia) return;
    if (!playingFiles.length) return;
    const next = Math.min(playingFiles.length - 1, Math.max(0, playingFileIndex) + 1);
    if (next === playingFileIndex) return;
    playFromPlaylistIndex(next);
  }, [playFromPlaylistIndex, playingFileIndex, playingFiles.length, playingMedia]);

  const handlePrevPlayingItem = useCallback(() => {
    if (!playingMedia) return;
    if (!playingFiles.length) return;
    const prev = Math.max(0, Math.max(0, playingFileIndex) - 1);
    if (prev === playingFileIndex) return;
    playFromPlaylistIndex(prev);
  }, [playFromPlaylistIndex, playingFileIndex, playingFiles.length, playingMedia]);

  const handleContinueNextSeries = useCallback(
    async (nextId: number) => {
      if (!Number.isFinite(nextId)) return;
      await handlePlayById(nextId);
    },
    [handlePlayById],
  );

  const handleSelectEpisode = useCallback(
    (seasonKey: string, ep: any) => {
      const section = playerSeasons.find((s) => s.key === seasonKey);
      if (!section || !ep?.path) return;
      const nextPath = String(ep.path);
      const nextFiles = section.episodes || [];
      const baseItem = section.baseItem || playingMedia;
      setPlayingMedia({ ...(baseItem as any), path: nextPath, files: nextFiles });
      try {
        const src = MediaService.getStreamUrlFromPath(nextPath);
        setPlayingSrc(src);
      } catch {}
    },
    [playerSeasons, playingMedia],
  );

  const closePlayer = useCallback(() => {
    setPlayingMedia(null);
    setPlayingSrc(null);
  }, []);

  const hasPrevItem = playingFiles.length > 1 && playingFileIndex > 0;
  const hasNextItem = playingFiles.length > 1 && playingFileIndex >= 0 && playingFileIndex < playingFiles.length - 1;

  return {
    playingMedia,
    playingSrc,
    playerSeasons,
    nowPlayingLabel,
    handlePlay,
    handlePlayById,
    handleNextPlayingItem,
    handlePrevPlayingItem,
    handleContinueNextSeries,
    handleSelectEpisode,
    closePlayer,
    hasPrevItem,
    hasNextItem,
  };
};
