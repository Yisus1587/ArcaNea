import React, { useEffect } from 'react';
import { ArcaneaCircuitLoader } from './ArcaneaCircuitLoader';
import { useI18n } from '../../i18n/i18n';

interface SplashScreenProps {
  onComplete: () => void;
}

export const SplashScreen: React.FC<SplashScreenProps> = ({ onComplete }) => {
  const { t } = useI18n();
  useEffect(() => {
    // Kick off the real boot logic immediately; keep the splash visible briefly for polish.
    const t = window.setTimeout(() => {
      onComplete();
    }, 300);
    return () => window.clearTimeout(t);
  }, [onComplete]);

  return (
    <div className="fixed inset-0 z-[100] bg-[#0b1220] flex flex-col items-center justify-center">
      <div className="relative mb-8 w-44 h-44">
        <ArcaneaCircuitLoader className="w-full h-full" strokeWidth={14} />
      </div>
      
      <h1 className="text-4xl font-bold text-white mb-2 tracking-tight bg-clip-text text-transparent bg-gradient-to-r from-white to-slate-400">
        ArcaNea
      </h1>
      <p className="text-slate-500 text-sm font-medium tracking-widest uppercase mb-12">{t('splash_tagline')}</p>

      <div className="w-72 space-y-4">
        <div className="flex items-center justify-center text-indigo-300 h-6 transition-all duration-300">
          <span className="text-sm font-mono">{t('starting')}</span>
        </div>
        <div className="h-1 w-full bg-slate-800 rounded-full overflow-hidden">
          <div className="h-full w-1/2 bg-gradient-to-r from-indigo-500 to-purple-500 animate-pulse" />
        </div>
      </div>
    </div>
  );
};
