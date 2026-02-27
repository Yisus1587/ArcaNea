import { useCallback, useState } from 'react';
import { Notification } from '../types';

type NotificationType = 'success' | 'error' | 'info';

export const useNotifications = () => {
  const [notification, setNotification] = useState<Notification | null>(null);

  const showNotification = useCallback((type: NotificationType, message: string) => {
    const msg = String(message || '');
    const friendly = (() => {
      const lower = msg.toLowerCase();
      if (type === 'success') {
        if (lower.includes('bienvenido')) return `👋 ${msg}`;
        if (lower.includes('perfil cambiado')) return `✅ ${msg}`;
        if (lower.includes('configuración')) return `✅ ${msg}`;
        if (lower.includes('biblioteca actualizada')) return `🔄 ${msg}`;
        if (lower.includes('enriquecimiento')) return `🧠 ${msg}`;
      }
      if (type === 'info') {
        if (lower.includes('escaneo')) return `🔎 ${msg}`;
        if (lower.includes('ruta')) return `📁 ${msg}`;
      }
      if (type === 'error') {
        return `⚠️ ${msg}`;
      }
      return msg;
    })();

    const lowerFriendly = friendly.toLowerCase();
    const isWelcome = lowerFriendly.includes('bienvenido') || lowerFriendly.includes('welcome');
    const isQuick =
      isWelcome ||
      lowerFriendly.includes('perfil cambiado') ||
      lowerFriendly.includes('biblioteca') ||
      lowerFriendly.includes('escaneo');
    const durationMs = isQuick ? 3000 : 3500;

    setNotification({
      id: Date.now().toString(),
      type,
      message: friendly,
      durationMs,
      variant: isWelcome ? 'welcome' : 'default',
    });
  }, []);

  const clearNotification = useCallback(() => setNotification(null), []);

  return { notification, showNotification, clearNotification };
};
