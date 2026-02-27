import React, { useEffect } from 'react';
import { X, BookOpen } from 'lucide-react';
import { Button } from '../ui/Button';
import { useI18n } from '../../i18n/i18n';

export function HelpModal(props: { open: boolean; onClose: () => void }) {
  const { open, onClose } = props;
  const { t } = useI18n();

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-[240] flex items-center justify-center bg-black/70 backdrop-blur-sm p-4">
      <div className="w-full max-w-4xl rounded-2xl border border-white/10 bg-slate-950/90 shadow-2xl overflow-hidden">
        <div className="flex items-center justify-between px-5 py-4 border-b border-white/10">
          <div className="min-w-0 flex items-center gap-3">
            <div className="w-9 h-9 rounded-lg bg-indigo-600/20 border border-indigo-500/20 flex items-center justify-center text-indigo-200">
              <BookOpen size={18} />
            </div>
            <div>
              <div className="text-white font-semibold">{t('quick_start_title')}</div>
              <div className="text-xs text-slate-400">{t('quick_start_subtitle')}</div>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-2 rounded-lg text-slate-300 hover:text-white hover:bg-white/10 transition"
            aria-label={t('close')}
          >
            <X size={18} />
          </button>
        </div>

        <div className="max-h-[70vh] overflow-y-auto px-6 py-5 text-sm text-slate-300 space-y-4">
          <div className="text-slate-400">{t('quick_start_intro')}</div>
          <ol className="list-decimal list-inside space-y-2">
            <li>{t('help_step_1')}</li>
            <li>{t('help_step_2')}</li>
            <li>{t('help_step_3')}</li>
            <li>{t('help_step_4')}</li>
            <li>{t('help_step_5')}</li>
            <li>{t('help_step_6')}</li>
            <li>{t('help_step_7')}</li>
            <li>{t('help_step_8')}</li>
          </ol>
          <div className="text-xs text-slate-500">{t('quick_start_tip')}</div>
        </div>

        <div className="px-5 py-4 border-t border-white/10 flex items-center justify-end gap-2">
          <Button variant="ghost" onClick={onClose}>{t('close')}</Button>
        </div>
      </div>
    </div>
  );
}
