import React, { useEffect, useMemo, useState } from 'react';
import { AlertTriangle, Loader2, RefreshCw, X } from 'lucide-react';
import { Button } from '../ui/Button';
import { MediaItem } from '../../types';
import { MediaService } from '../../services/api';
import { ManualMappingModal } from '../media/ManualMappingModal';
import { useI18n } from '../../i18n/i18n';

export function NoMatchModal(props: {
  open: boolean;
  onClose: () => void;
  onChanged?: () => void;
}) {
  const { open, onClose, onChanged } = props;
  const { t } = useI18n();
  const [items, setItems] = useState<MediaItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [busyId, setBusyId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [manualItem, setManualItem] = useState<MediaItem | null>(null);
  const [showManual, setShowManual] = useState(false);
  const [brokenImages, setBrokenImages] = useState<Record<string, boolean>>({});

  const visibleItems = useMemo(() => items || [], [items]);

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await MediaService.getNoMatchItems(1, 200);
      setItems(res.items || []);
    } catch (e: any) {
      setError(String(e?.message || e));
      setItems([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (!open) return;
    void load();
  }, [open]);

  const handleOmit = async (id: string) => {
    setBusyId(id);
    try {
      const res = await MediaService.omitMediaItem(Number(id));
      if (!res.ok) throw new Error(res.detail || 'omit_failed');
      setItems(prev => prev.filter((i) => String(i.id) !== String(id)));
      if (onChanged) onChanged();
    } catch (e: any) {
      setError(String(e?.message || e));
    } finally {
      setBusyId(null);
    }
  };

  const handleManual = async (id: string) => {
    setBusyId(id);
    try {
      const full = await MediaService.getById(Number(id));
      setManualItem(full);
      setShowManual(true);
    } catch (e: any) {
      setError(String(e?.message || e));
    } finally {
      setBusyId(null);
    }
  };

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-[240] flex items-center justify-center bg-black/70 backdrop-blur-sm p-4">
      <div className="w-full max-w-5xl rounded-2xl border border-white/10 bg-slate-950/90 shadow-2xl overflow-hidden">
        <div className="flex items-center justify-between px-5 py-4 border-b border-white/10">
          <div className="min-w-0">
            <div className="text-white font-semibold">{t('no_match_review_title')}</div>
            <div className="mt-1 text-xs text-slate-400">{t('no_match_review_subtitle')}</div>
          </div>
          <div className="flex items-center gap-2">
            <Button size="sm" variant="ghost" onClick={() => void load()} disabled={loading} icon={<RefreshCw size={14} />}>
              {t('reload')}
            </Button>
            <button
              onClick={onClose}
              className="p-2 rounded-lg text-slate-300 hover:text-white hover:bg-white/10 transition"
              aria-label={t('close')}
            >
              <X size={18} />
            </button>
          </div>
        </div>

        <div className="max-h-[65vh] overflow-y-auto px-5 py-4">
          {loading ? (
            <div className="flex items-center justify-center py-16 text-slate-300">
              <Loader2 className="animate-spin mr-3" size={18} />
              {t('loading')}
            </div>
          ) : error ? (
            <div className="px-4 py-8 text-center text-sm text-red-300">
              {error}
            </div>
          ) : visibleItems.length === 0 ? (
            <div className="px-4 py-10 text-center text-sm text-slate-400">
              {t('no_match_empty')}
            </div>
          ) : (
            <div className="space-y-3">
              {visibleItems.map((it) => {
                const id = String(it.id);
                const poster = (it.posterPath || it.thumbnailUrl || '').trim();
                const posterOk = poster && !brokenImages[id];
                const pathLabel = it.path || '';
                return (
                  <div key={id} className="flex items-center gap-4 p-3 rounded-xl border border-white/10 bg-white/5">
                    <div className="w-14 h-20 rounded-lg overflow-hidden border border-white/10 bg-slate-900 flex items-center justify-center">
                      {posterOk ? (
                        <img
                          src={poster}
                          className="w-full h-full object-cover"
                          onError={() => setBrokenImages(prev => ({ ...prev, [id]: true }))}
                        />
                      ) : (
                        <AlertTriangle size={20} className="text-slate-500" />
                      )}
                    </div>
                    <div className="min-w-0 flex-1">
                      <div className="text-white font-medium truncate">{it.title || t('unknown')}</div>
                      <div className="text-[11px] text-slate-500 font-mono truncate" title={pathLabel}>
                        {pathLabel}
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <Button
                        size="sm"
                        onClick={() => void handleManual(id)}
                        disabled={busyId === id}
                      >
                        {t('fix_match')}
                      </Button>
                      <Button
                        size="sm"
                        variant="ghost"
                        onClick={() => void handleOmit(id)}
                        disabled={busyId === id}
                      >
                        {t('omit')}
                      </Button>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>

      {showManual && manualItem ? (
        <ManualMappingModal
          item={manualItem}
          onClose={() => {
            setShowManual(false);
            setManualItem(null);
          }}
          onSaved={() => {
            setShowManual(false);
            setManualItem(null);
            void load();
            if (onChanged) onChanged();
          }}
        />
      ) : null}
    </div>
  );
}
