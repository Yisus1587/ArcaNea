import React, { useEffect } from 'react';
import { CheckCircle, AlertCircle, Info, X } from 'lucide-react';
import { Notification } from '../../types';

interface ToastProps {
  notification: Notification;
  onClose: () => void;
  className?: string;
}

export const Toast: React.FC<ToastProps> = ({ notification, onClose, className }) => {
  useEffect(() => {
    const timer = setTimeout(onClose, notification.durationMs ?? 3500);
    return () => clearTimeout(timer);
  }, [onClose, notification.durationMs]);

  const icons = {
    success: <CheckCircle className="text-green-400" size={20} />,
    error: <AlertCircle className="text-red-400" size={20} />,
    info: <Info className="text-blue-400" size={20} />
  };

  const borders = {
    success: 'border-green-500/20',
    error: 'border-red-500/20',
    info: 'border-blue-500/20'
  };

  const isWelcome = notification.variant === 'welcome';
  const shape = isWelcome ? 'rounded-full px-4 py-2' : 'rounded-lg sm:rounded-xl p-3 sm:p-4';

  return (
    <div className={`flex items-center ${shape} bg-[#1e293b]/90 backdrop-blur-md border ${borders[notification.type]} shadow-2xl animate-slide-up w-full sm:max-w-sm ${className || ''}`}>
      <div className="mr-3 shrink-0">
        {icons[notification.type]}
      </div>
      <p className="text-xs sm:text-sm font-medium text-slate-100 mr-10">
        {notification.message}
      </p>
      {!isWelcome ? (
        <button 
          onClick={onClose}
          className="absolute top-2 right-2 w-8 h-8 flex items-center justify-center rounded-full hover:bg-white/10 text-slate-400 hover:text-white transition-colors"
        >
          <X size={14} />
        </button>
      ) : null}
    </div>
  );
};
