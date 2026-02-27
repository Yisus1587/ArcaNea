import React from 'react';
import { Notification } from '../../types';
import { Toast } from './Toast';

interface NotificationHostProps {
  notification: Notification | null;
  onClose: () => void;
}

export const NotificationHost: React.FC<NotificationHostProps> = ({ notification, onClose }) => {
  if (!notification) return null;
  return (
    <div className="fixed bottom-4 left-4 right-4 sm:left-auto sm:right-6 sm:bottom-6 z-[200] flex flex-col items-stretch sm:items-end gap-2">
      <Toast notification={notification} onClose={onClose} />
    </div>
  );
};
