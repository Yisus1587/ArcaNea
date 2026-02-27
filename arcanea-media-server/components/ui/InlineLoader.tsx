import React from 'react';
import { Loader2 } from 'lucide-react';
import { useI18n } from '../../i18n/i18n';

interface InlineLoaderProps {
  label?: string;
  className?: string;
}

export const InlineLoader: React.FC<InlineLoaderProps> = ({ label, className }) => {
  const { t } = useI18n();
  const resolvedLabel = label ?? t('loading');
  return (
    <div className={`flex items-center justify-center py-6 text-sm text-slate-400 ${className || ''}`}>
      <Loader2 className="animate-spin mr-2" size={16} />
      {resolvedLabel}
    </div>
  );
};
