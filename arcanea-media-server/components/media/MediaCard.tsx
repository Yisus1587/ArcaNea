import React from 'react';
import { Play, Star, ImageOff } from 'lucide-react';
import { MediaItem } from '../../types';
import { useI18n } from '../../i18n/i18n';

interface MediaCardProps {
  item: MediaItem;
  onClick: (item: MediaItem) => void;
}

const MediaCardImpl: React.FC<MediaCardProps> = ({ item, onClick }) => {
  const { t } = useI18n();
  const fallbackPoster = '/images/arcanea-poster.svg';
  const hasImage = !!((item as any).posterPath || item.thumbnailUrl || item.backdropUrl);
  const imageSrc = (item as any).posterPath || item.thumbnailUrl || item.backdropUrl || fallbackPoster;
  return (
    <div 
      className="group relative flex flex-col cursor-pointer animate-fade-in transition-transform duration-300 hover:-translate-y-1"
      onClick={() => onClick(item)}
    >
      {/* Image Container with Aspect Ratio 2:3 */}
      <div className="relative w-full aspect-[2/3] rounded-xl overflow-hidden shadow-xl bg-slate-800 border border-white/5 transition-all duration-300 group-hover:shadow-indigo-500/20 group-hover:scale-[1.02] group-hover:border-indigo-500/30">
        <img
          src={imageSrc}
          alt={item.title}
          className="w-full h-full object-cover transition-transform duration-500 group-hover:scale-110"
          loading="lazy"
          decoding="async"
          fetchpriority="low"
        />
        {!hasImage && (
          <div className="absolute inset-0 flex items-center justify-center text-slate-200/70">
            <div className="flex items-center gap-2 text-xs px-2 py-1 rounded-full bg-black/40 backdrop-blur-md border border-white/10">
              <ImageOff size={14} />
              {t('no_image')}
            </div>
          </div>
        )}
        
        {/* Gradient Overlay */}
        <div className="absolute inset-0 bg-gradient-to-t from-black/80 via-transparent to-transparent opacity-60 group-hover:opacity-80 transition-opacity" />

        {/* Hover Action Overlay */}
        <div className="absolute inset-0 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-all duration-300 bg-black/40 backdrop-blur-[2px]">
          <div className="w-12 h-12 rounded-full bg-white/10 backdrop-blur-md border border-white/20 flex items-center justify-center shadow-2xl transform scale-50 group-hover:scale-100 transition-transform duration-300 hover:bg-indigo-600 hover:border-indigo-500">
            <Play fill="white" className="text-white ml-1" size={20} />
          </div>
        </div>

        {/* Top Right Rating Badge */}
        {item.rating && (
          <div className="absolute top-2 right-2 px-2 py-1 rounded-lg bg-black/40 backdrop-blur-xl border border-white/10 flex items-center space-x-1">
            <Star size={10} className="text-yellow-400 fill-yellow-400" />
            <span className="text-xs font-bold text-white">{item.rating.toFixed(1)}</span>
          </div>
        )}

        
      </div>

      {/* Info */}
      <div className="mt-2 sm:mt-3 px-0.5 sm:px-1">
        <h3 className="text-xs sm:text-sm font-semibold text-white truncate group-hover:text-indigo-400 transition-colors">
          {item.title}
        </h3>
        <div className="flex items-center justify-between mt-1">
           <p className="text-xs text-slate-400">
            {item.year}
          </p>
          <span className="text-[10px] uppercase tracking-wider text-slate-500 border border-slate-700 px-1 rounded">
            {item.type}
          </span>
        </div>
       
      </div>
    </div>
  );
};

export const MediaCard = React.memo(MediaCardImpl);
