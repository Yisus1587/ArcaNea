import React from 'react';
import { Play, Info } from 'lucide-react';
import { Button } from '../ui/Button';
import { MediaItem } from '../../types';
import { useI18n } from '../../i18n/i18n';

interface FeaturedHeroProps {
  item: MediaItem;
  onPlay: (item: MediaItem) => void;
  onInfo: (item: MediaItem) => void;
}

export const FeaturedHero: React.FC<FeaturedHeroProps> = ({ item, onPlay, onInfo }) => {
  const { t } = useI18n();
  return (
    <div className="relative h-[260px] sm:h-[340px] lg:h-[420px] rounded-3xl overflow-hidden shadow-2xl group cursor-pointer mb-6 sm:mb-10 mt-8 sm:mt-0" onClick={() => onInfo(item)}>
      <img 
        src={item.backdropUrl || item.thumbnailUrl} 
        alt={`${t('featured')}: ${item.title}`} 
        className="w-full h-full object-cover transition-transform duration-700 group-hover:scale-105"
      />
      <div className="absolute inset-0 bg-gradient-to-t from-[#18181b] via-[#18181b]/60 to-transparent" />
      <div className="absolute inset-0 bg-gradient-to-r from-[#18181b]/70 via-transparent to-transparent" />
      <div className="absolute bottom-0 left-0 p-5 sm:p-10 max-w-2xl">
        <span className="px-2 py-1 bg-indigo-600 rounded text-xs font-bold uppercase tracking-wider mb-2 inline-block">{t('featured')}</span>
        <h1 className="text-2xl sm:text-5xl lg:text-6xl font-black mb-3 sm:mb-4 leading-tight drop-shadow-2xl text-white line-clamp-2">{item.title}</h1>
        <p className="text-slate-300 text-sm sm:text-lg line-clamp-2 mb-4 sm:mb-6 drop-shadow-md">{item.overview}</p>
        <div className="flex space-x-3">
            <Button 
                onClick={(e) => { e.stopPropagation(); onPlay(item); }} 
                icon={<Play fill="currentColor" size={18} />}
                className="bg-white text-black shadow-[0_10px_30px_rgba(0,0,0,0.35)] hover:bg-white/90"
            >
                {t('play')}
            </Button>
            <Button 
                variant="ghost" 
                onClick={(e) => { e.stopPropagation(); onInfo(item); }}
                icon={<Info size={18} />}
                className="bg-white/10 backdrop-blur-md border border-white/20 text-white hover:bg-white/20"
            >
                {t('info')}
            </Button>
        </div>
      </div>
    </div>
  );
};
