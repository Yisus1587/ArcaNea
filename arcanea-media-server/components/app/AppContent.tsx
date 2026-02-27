import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { ArrowUpDown, Loader2, RefreshCw, Search, ServerCrash } from 'lucide-react';
import { MediaCard } from '../media/MediaCard';
import { FeaturedHero } from '../media/FeaturedHero';
import { SettingsView } from '../settings/SettingsView';
import { InlineLoader } from '../ui/InlineLoader';
import { Button } from '../ui/Button';
import { Drive, MediaItem, MetadataConfig, ScanStatus, UserProfile, ViewState } from '../../types';
import { useI18n } from '../../i18n/i18n';

type SortOption = 'date' | 'title' | 'rating';

interface AppContentProps {
  currentView: ViewState;
  currentUser: UserProfile | null;
  drives: Drive[];
  libraryPaths: string[];
  scanStatus: ScanStatus;
  enrichment: any | null;
  migrationRunning: boolean;
  migrationError: string | null;
  metadataConfig: MetadataConfig;
  profiles: UserProfile[];
  error: string | null;
  loading: boolean;
  profileLoadingHint: boolean;
  searchQuery: string;
  lang: string;
  baseItems: MediaItem[];
  userListLoading: boolean;
  suggestedItems: MediaItem[];
  hasMoreMedia: boolean;
  loadingMoreMedia: boolean;
  mediaTotal: number;
  scrollContainerRef: React.RefObject<HTMLDivElement>;
  sentinelRef: React.RefObject<HTMLDivElement>;
  recommendedItems: MediaItem[];
  recommendedForYou: MediaItem[];
  inProgressItems: MediaItem[];
  trendingItems: MediaItem[];
  onChangeView: (view: ViewState) => void;
  onStartEnrich: () => void;
  onStartScan: () => void;
  onAddPath: (path: string) => void;
  onRemovePath: (path: string) => void;
  onUpdateConfig: (cfg: MetadataConfig) => void;
  onRefreshSettings: () => void;
  onAddProfile: (profile: Omit<UserProfile, 'id'>) => void;
  onUpdateProfile: (profile: UserProfile) => void;
  onDeleteProfile: (id: string) => void;
  onSelectMedia: (item: MediaItem) => void;
  onPlay: (item: MediaItem) => void;
  onRefreshMedia: () => void;
}

export const AppContent: React.FC<AppContentProps> = ({
  currentView,
  currentUser,
  drives,
  libraryPaths,
  scanStatus,
  enrichment,
  migrationRunning,
  migrationError,
  metadataConfig,
  profiles,
  error,
  loading,
  profileLoadingHint,
  searchQuery,
  baseItems,
  userListLoading,
  suggestedItems,
  hasMoreMedia,
  loadingMoreMedia,
  mediaTotal,
  scrollContainerRef,
  sentinelRef,
  recommendedItems,
  recommendedForYou,
  inProgressItems,
  trendingItems,
  onChangeView,
  onStartEnrich,
  onStartScan,
  onAddPath,
  onRemovePath,
  onUpdateConfig,
  onRefreshSettings,
  onAddProfile,
  onUpdateProfile,
  onDeleteProfile,
  onSelectMedia,
  onPlay,
  onRefreshMedia,
}) => {
  const { t } = useI18n();
  const [sortBy, setSortBy] = useState<SortOption>('date');
  const [gridCols, setGridCols] = useState(6);
  const [rowHeight, setRowHeight] = useState(300);
  const [scrollTop, setScrollTop] = useState(0);

  const computeGrid = useCallback(() => {
    try {
      const w = window.innerWidth;
      let cols = 6;
      if (w < 640) cols = 2;
      else if (w < 768) cols = 3;
      else if (w < 1024) cols = 4;
      else if (w < 1280) cols = 5;
      else cols = 6;

      const container = scrollContainerRef.current;
      const containerWidth = container ? container.clientWidth : window.innerWidth;
      const gap = w < 640 ? 12 : 24;
      const usable = Math.max(0, containerWidth - gap * (cols - 1));
      const cardWidth = cols > 0 ? usable / cols : containerWidth;
      const cardHeight = cardWidth * 1.5 + 58; // 2:3 image + text block

      setGridCols(cols);
      setRowHeight(cardHeight);
    } catch {
      // ignore
    }
  }, [scrollContainerRef]);

  useEffect(() => {
    computeGrid();
    const onResize = () => computeGrid();
    window.addEventListener('resize', onResize);
    return () => window.removeEventListener('resize', onResize);
  }, [computeGrid]);

  useEffect(() => {
    const el = scrollContainerRef.current;
    if (!el) return;
    const onScroll = () => setScrollTop(el.scrollTop || 0);
    el.addEventListener('scroll', onScroll, { passive: true });
    return () => el.removeEventListener('scroll', onScroll);
  }, [scrollContainerRef]);

  const sortedMediaItems = useMemo(() => {
    const items = [...baseItems];
    switch (sortBy) {
      case 'title':
        return items.sort((a, b) => a.title.localeCompare(b.title));
      case 'rating':
        return items.sort((a, b) => (b.rating || 0) - (a.rating || 0));
      case 'date':
      default:
        return items.sort((a, b) => b.addedAt.localeCompare(a.addedAt));
    }
  }, [baseItems, sortBy]);

  const featuredItem = useMemo(() => {
    if (baseItems.length === 0) return null;
    const randomIndex = Math.floor(Math.random() * baseItems.length);
    return baseItems[randomIndex];
  }, [baseItems]);

  const renderHomeSection = (title: string, items: MediaItem[], subtitle?: string) => {
    if (!items || items.length === 0) return null;
    return (
      <div className="space-y-4">
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-xl font-semibold text-white">{title}</h3>
            {subtitle ? <div className="text-xs text-slate-400 mt-1">{subtitle}</div> : null}
          </div>
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-3 sm:gap-6">
          {items.map((item) => (
            <MediaCard key={item.id} item={item} onClick={onSelectMedia} />
          ))}
        </div>
      </div>
    );
  };

  if (currentView === 'settings') {
    if (!currentUser?.isManager) {
      return (
        <div className="flex flex-col items-center justify-center py-20 text-slate-400">
          <div className="w-16 h-16 bg-slate-800 rounded-full flex items-center justify-center mb-4">
            <ServerCrash size={32} />
          </div>
          <h2 className="text-xl font-semibold text-white mb-2">{t('access_restricted_title')}</h2>
          <p className="mb-6 text-center max-w-md">{t('access_settings_denied')}</p>
          <Button onClick={() => onChangeView('home')}>{t('back')}</Button>
        </div>
      );
    }
    return (
      <SettingsView
        drives={drives}
        libraryPaths={libraryPaths}
        scanStatus={scanStatus}
        enrichment={enrichment}
        migrationRunning={migrationRunning}
        migrationError={migrationError}
        onStartEnrich={onStartEnrich}
        metadataConfig={metadataConfig}
        profiles={profiles}
        onAddPath={onAddPath}
        onRemovePath={onRemovePath}
        onUpdateConfig={onUpdateConfig}
        onStartScan={onStartScan}
        onRefresh={onRefreshSettings}
        onAddProfile={onAddProfile}
        onUpdateProfile={onUpdateProfile}
        onDeleteProfile={onDeleteProfile}
      />
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-slate-400">
        <ServerCrash size={64} className="mb-4 text-red-500 opacity-50" />
        <h2 className="text-xl font-bold text-white mb-2">{t('connection_problem')}</h2>
        <p className="mb-6">{error}</p>
        <Button onClick={onRefreshMedia}>{t('retry')}</Button>
      </div>
    );
  }

  if (!loading && baseItems.length === 0 && searchQuery) {
    return (
      <div className="flex flex-col items-center justify-center py-20 text-slate-400">
        <div className="w-16 h-16 bg-slate-800 rounded-full flex items-center justify-center mb-4">
          <Search size={32} />
        </div>
        <h2 className="text-xl font-semibold text-white mb-2">{t('no_results_title')}</h2>
        <p>{t('no_results_for')} "{searchQuery}"</p>
        {suggestedItems.length > 0 ? (
          <div className="w-full mt-6">
            <div className="text-sm text-slate-300 mb-3">{t('did_you_mean')}</div>
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-3 sm:gap-6">
              {suggestedItems.map((it) => (
                <MediaCard key={`suggest-${it.id}`} item={it} onClick={onSelectMedia} />
              ))}
            </div>
          </div>
        ) : null}
      </div>
    );
  }

  if (!loading && currentView === 'list' && baseItems.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-20 text-slate-400">
        <div className="w-16 h-16 bg-slate-800 rounded-full flex items-center justify-center mb-4">
          <Search size={32} />
        </div>
        <h2 className="text-xl font-semibold text-white mb-2">{t('list_empty_title')}</h2>
        <p>{t('list_empty_desc')}</p>
      </div>
    );
  }

  if ((profileLoadingHint || loading) && currentView === 'home' && !searchQuery) {
    return (
      <div className="flex flex-col items-center justify-center py-20 text-slate-400">
        <Loader2 size={32} className="mb-4 text-indigo-400 animate-spin" />
        <h2 className="text-xl font-semibold text-white mb-2">
          {t('loading_recommendations_title')}
        </h2>
        <p>{t('loading_recommendations_desc')}</p>
      </div>
    );
  }

  if (!loading && currentView === 'home' && !searchQuery && baseItems.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-20 text-slate-400">
        <div className="w-16 h-16 bg-slate-800 rounded-full flex items-center justify-center mb-4">
          <Search size={32} />
        </div>
        <h2 className="text-xl font-semibold text-white mb-2">{t('no_content_title')}</h2>
        <p>{t('no_content_desc')}</p>
      </div>
    );
  }

  const isHomeView = currentView === 'home' && !searchQuery;
  const overscan = 3;
  const totalItems = sortedMediaItems.length;
  const safeRowHeight = Math.max(1, rowHeight);
  const totalRows = gridCols > 0 ? Math.ceil(totalItems / gridCols) : 0;
  const startRow = Math.max(0, Math.floor(scrollTop / safeRowHeight) - overscan);
  const endRow = Math.min(
    totalRows,
    Math.ceil((scrollTop + (scrollContainerRef.current?.clientHeight || 800)) / safeRowHeight) + overscan,
  );
  const startIndex = startRow * gridCols;
  const endIndex = Math.min(totalItems, endRow * gridCols);
  const visibleItems = sortedMediaItems.slice(startIndex, endIndex);
  const padTop = startRow * safeRowHeight;
  const padBottom = Math.max(0, totalRows * safeRowHeight - endRow * safeRowHeight);

  return (
    <div className="space-y-8 animate-fade-in relative">
      {loading && baseItems.length > 0 && (
        <div className="absolute inset-0 z-10 bg-[#0b1220]/50 backdrop-blur-sm flex items-start justify-center pt-20">
          <div className="flex items-center space-x-2 bg-slate-800 px-4 py-2 rounded-full shadow-lg border border-white/10">
            <Loader2 className="animate-spin text-indigo-400" size={16} />
            <span className="text-sm font-medium text-white">{t('updating')}</span>
          </div>
        </div>
      )}

      {currentView === 'home' && !searchQuery && featuredItem && (
        <FeaturedHero item={featuredItem} onPlay={onPlay} onInfo={onSelectMedia} />
      )}

      {isHomeView ? (
        <div className="space-y-10">
          {renderHomeSection(
            t('home_recommended_title'),
            recommendedForYou && recommendedForYou.length ? recommendedForYou : recommendedItems,
            t('home_recommended_subtitle'),
          )}
          {renderHomeSection(t('home_in_progress_title'), inProgressItems, t('home_in_progress_subtitle'))}
          {renderHomeSection(t('home_trending_title'), trendingItems, t('home_trending_subtitle'))}
        </div>
      ) : null}

      <div className="flex items-center justify-between gap-3 flex-wrap">
        <h2 className="text-2xl sm:text-2xl font-bold text-white">
          {searchQuery
            ? `${t('results_for')} "${searchQuery}"`
            : currentView === 'home'
              ? t('recently_added')
              : currentView === 'movies'
                ? t('all_movies')
                : currentView === 'list'
                  ? t('my_list')
                  : t('series_and_tv')}
        </h2>

        <div className="flex items-center space-x-3">
          {currentUser?.isKid && (
            <div className="hidden sm:flex items-center px-3 py-1.5 rounded-lg bg-emerald-500/10 border border-emerald-500/20 text-emerald-300 text-xs font-semibold">
              {t('kid_mode_active')}
            </div>
          )}
          <div className="flex items-center space-x-2 bg-slate-800/50 rounded-lg px-3 py-1.5 border border-white/5 hover:bg-slate-800 transition-colors">
            <ArrowUpDown size={14} className="text-slate-400" />
            <select
              value={sortBy}
              onChange={(e) => setSortBy(e.target.value as SortOption)}
              className="bg-transparent text-sm text-slate-200 focus:outline-none cursor-pointer [&>option]:bg-[#1e293b]"
            >
              <option value="date">{t('sort_added')}</option>
              <option value="title">{t('sort_title')}</option>
              <option value="rating">{t('sort_rating')}</option>
            </select>
          </div>
          <Button variant="ghost" size="sm" icon={<RefreshCw size={14} />} onClick={onRefreshMedia}>
            {t('refresh')}
          </Button>
        </div>
      </div>

      {loading && baseItems.length === 0 && currentView !== 'list' ? (
        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-3 sm:gap-6 pb-16 sm:pb-20">
          {Array.from({ length: 12 }).map((_, i) => (
            <div key={i} className="flex flex-col space-y-3 animate-pulse">
              <div className="w-full aspect-[2/3] bg-slate-800/50 rounded-xl border border-white/5" />
              <div className="px-1 space-y-2">
                <div className="h-4 bg-slate-800/50 rounded w-3/4" />
              </div>
            </div>
          ))}
        </div>
      ) : (
        <>
          <div className="pb-8 sm:pb-10" style={{ paddingTop: padTop, paddingBottom: padBottom }}>
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-3 sm:gap-6">
              {visibleItems.filter(Boolean).map((item) => (
                <MediaCard key={item.id} item={item} onClick={onSelectMedia} />
              ))}
            </div>
          </div>
          {currentView === 'list' && userListLoading ? <InlineLoader label={t('loading_list')} /> : null}
          {loadingMoreMedia ? <InlineLoader label={t('loading_more')} /> : null}
          <div ref={sentinelRef} className="h-2" />
          {!hasMoreMedia && baseItems.length > 0 ? (
            <div className="flex items-center justify-center py-6 text-xs text-slate-500">
              {t('showing')} {baseItems.length}
              {currentView === 'list' ? '' : mediaTotal ? ` ${t('of')} ${mediaTotal}` : ''}.
            </div>
          ) : null}
        </>
      )}
    </div>
  );
};
