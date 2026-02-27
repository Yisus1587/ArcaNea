import React, { useState, useRef, useEffect } from 'react';
import { Home, Film, Tv, Settings, Search, LogOut, X, ChevronUp, Check, User, Pin, PinOff, Bookmark, ChevronLeft } from 'lucide-react';
import { ViewState, UserProfile } from '../../types';
import { useI18n } from '../../i18n/i18n';

interface SidebarProps {
  currentView: ViewState;
  onChangeView: (view: ViewState) => void;
  isOpen: boolean;
  onRequestOpen?: () => void;
  onRequestClose?: () => void;
  pinned: boolean;
  onTogglePinned: () => void;
  currentUser: UserProfile | null;
  onLogout: () => void;
  searchQuery: string;
  onSearch: (query: string) => void;
  profiles: UserProfile[];
  onSwitchProfile: (profile: UserProfile) => void;
  scanRunning?: boolean;
  scanProgress?: number | null;
}

export const Sidebar: React.FC<SidebarProps> = ({ 
  currentView, 
  onChangeView, 
  isOpen,
  onRequestOpen,
  onRequestClose,
  pinned,
  onTogglePinned,
  currentUser, 
  onLogout,
  searchQuery,
  onSearch,
  profiles,
  onSwitchProfile,
  scanRunning,
  scanProgress,
}) => {
  const { t } = useI18n();
  const [isProfileMenuOpen, setIsProfileMenuOpen] = useState(false);
  const [isMobile, setIsMobile] = useState(false);
  const profileMenuRef = useRef<HTMLDivElement>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const requestFocusSearchRef = useRef(false);

  useEffect(() => {
    const mq = window.matchMedia('(max-width: 639px)');
    const apply = () => setIsMobile(!!mq.matches);
    apply();
    try {
      mq.addEventListener('change', apply);
      return () => mq.removeEventListener('change', apply);
    } catch (e) {
      // Safari/old fallback
      // eslint-disable-next-line deprecation/deprecation
      mq.addListener(apply);
      // eslint-disable-next-line deprecation/deprecation
      return () => mq.removeListener(apply);
    }
  }, []);

  // Close menu when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (profileMenuRef.current && !profileMenuRef.current.contains(event.target as Node)) {
        setIsProfileMenuOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // If we programmatically requested opening the sidebar for search, focus the input after it renders.
  useEffect(() => {
    if (!isOpen) return;
    if (!requestFocusSearchRef.current) return;
    requestFocusSearchRef.current = false;
    window.setTimeout(() => {
      searchInputRef.current?.focus();
    }, 60);
  }, [isOpen]);

  const navItems = [
    { id: 'home', label: t('nav_home'), icon: Home },
    { id: 'list', label: t('nav_list'), icon: Bookmark },
    { id: 'movies', label: t('nav_movies'), icon: Film },
    { id: 'tv', label: t('nav_series'), icon: Tv },
    ...(currentUser?.isManager ? [{ id: 'settings', label: t('nav_settings'), icon: Settings }] : []),
  ];

  return (
    <>
    <aside 
      data-arcanea-sidebar="1"
      className={`hidden sm:flex fixed top-0 left-0 h-full z-40 bg-[#0b1220]/95 backdrop-blur-xl border-r border-white/5 transition-all duration-300 flex-col ${
        isOpen ? 'w-56 sm:w-64' : 'w-16 sm:w-20'
      }`}
    >
      {/* Logo Area */}
      <div className="h-16 sm:h-20 flex items-center px-4 sm:px-6 border-b border-white/5">
        <button
          type="button"
          onClick={() => {
            if (!isMobile) return;
            if (isOpen) onRequestClose?.();
            else onRequestOpen?.();
          }}
          className="w-8 h-8 rounded-lg bg-gradient-to-br from-indigo-500 to-purple-600 flex items-center justify-center shadow-lg shadow-indigo-500/20 shrink-0"
          aria-label={t('sidebar_open')}
        >
          <img src="/icons/arcanea-icon.svg" alt="ArcaNea" className="w-5 h-5" />
        </button>
        <span className={`ml-3 font-bold text-xl tracking-tight bg-clip-text text-transparent bg-gradient-to-r from-white to-slate-400 transition-all duration-300 ${isOpen ? 'opacity-100 w-auto' : 'opacity-0 w-0 overflow-hidden'}`}>
          ArcaNea
        </span>
        {isOpen && (
          <div className="ml-auto flex items-center">
            {scanRunning ? (
              <div className="hidden sm:flex items-center gap-2 text-[11px] text-slate-300 bg-white/5 border border-white/10 rounded-full px-2 py-1 mr-2">
                <span className="inline-block h-1.5 w-1.5 rounded-full bg-indigo-500 animate-pulse" />
                {typeof scanProgress === 'number' ? `${scanProgress}%` : '...'}
              </div>
            ) : null}
            {isOpen && (
              <button
                type="button"
                onClick={() => onRequestClose?.()}
                className="p-2 rounded-lg transition-colors border bg-white/0 border-white/5 text-slate-400 hover:text-white hover:bg-white/5 mr-2"
                title={t('sidebar_close')}
              >
                <ChevronLeft size={16} />
              </button>
            )}
            {isMobile && !pinned ? (
              <button
                type="button"
                onClick={() => onRequestClose?.()}
                className="p-2 rounded-lg transition-colors border bg-white/0 border-white/5 text-slate-400 hover:text-white hover:bg-white/5 mr-2"
                title={t('sidebar_close')}
              >
                <X size={16} />
              </button>
            ) : null}
            <button
              type="button"
              onClick={onTogglePinned}
              className={`p-2 rounded-lg transition-colors border ${
                pinned
                  ? 'bg-indigo-600/10 border-indigo-500/20 text-indigo-300 hover:bg-indigo-600/20'
                  : 'bg-white/0 border-white/5 text-slate-400 hover:text-white hover:bg-white/5'
              }`}
              title={pinned ? t('sidebar_unpin') : t('sidebar_pin')}
            >
              {pinned ? <PinOff size={16} /> : <Pin size={16} />}
            </button>
          </div>
        )}
      </div>

      {/* Navigation */}
      <nav className="flex-1 py-6 space-y-2 px-3 overflow-hidden">
        {/* Persistent Search Bar */}
        <div className="px-3 mb-6 min-h-[40px]">
          {isOpen ? (
            <div className="relative group animate-fade-in">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400 group-focus-within:text-indigo-400 pointer-events-none" size={16} />
              <input 
                ref={searchInputRef}
                type="text" 
                placeholder={t('search_placeholder')} 
                value={searchQuery}
                onChange={(e) => onSearch(e.target.value)}
                onBlur={() => {
                  if (!pinned && onRequestClose) {
                    // In mobile, collapse after leaving search to recover space.
                    if (isMobile) onRequestClose();
                    // On desktop, collapse only if query is cleared.
                    else if (!searchQuery) onRequestClose();
                  }
                }}
                onKeyDown={(e) => {
                  if (e.key === 'Escape') {
                    e.preventDefault();
                    if (!pinned) onRequestClose?.();
                  }
                }}
                className="w-full bg-slate-800/50 border border-white/5 rounded-lg py-2 pl-10 pr-8 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:ring-1 focus:ring-indigo-500/50 transition-all"
              />
              {searchQuery && (
                <button 
                  onClick={() => {
                    onSearch('');
                    searchInputRef.current?.focus();
                  }}
                  className="absolute right-2 top-1/2 -translate-y-1/2 text-slate-500 hover:text-white p-1 rounded-full hover:bg-white/10 transition-colors"
                >
                  <X size={14} />
                </button>
              )}
            </div>
          ) : (
               <div className="flex justify-center group relative">
                  <div 
                    className="p-2.5 rounded-xl text-slate-400 hover:text-white hover:bg-white/5 transition-colors cursor-pointer"
                   onClick={() => {
                     requestFocusSearchRef.current = true;
                     onRequestOpen?.();
                   }}
                  >
                     <Search size={20} />
                  </div>
                {/* Tooltip for collapsed search */}
                <div className="absolute left-full top-1/2 -translate-y-1/2 ml-4 px-2 py-1 bg-slate-800 text-xs text-white rounded opacity-0 group-hover:opacity-100 pointer-events-none transition-opacity whitespace-nowrap z-50 border border-white/10">
                  {t('search')}
                </div>
             </div>
          )}
        </div>

        {navItems.map((item) => {
          const isActive = currentView === item.id;
          const Icon = item.icon;
          return (
            <button
              key={item.id}
              onClick={() => {
                onChangeView(item.id as ViewState);
                if (isMobile && !pinned) onRequestClose?.();
              }}
              className={`w-full flex items-center px-3 py-2 sm:py-3 rounded-xl transition-all duration-200 group relative overflow-hidden ${
                isActive 
                  ? 'bg-indigo-600/10 text-indigo-400' 
                  : 'text-slate-400 hover:text-white hover:bg-white/5'
              }`}
            >
              {isActive && (
                <div className="absolute left-0 top-0 bottom-0 w-1 bg-indigo-500 rounded-r-full" />
              )}
              <Icon size={18} className={`${isActive ? 'text-indigo-400' : 'text-slate-400 group-hover:text-white'} transition-colors shrink-0`} />
              <span className={`ml-3 font-medium transition-all duration-300 whitespace-nowrap ${isOpen ? 'opacity-100 translate-x-0' : 'opacity-0 -translate-x-4 absolute'}`}>
                {item.label}
              </span>
              
              {/* Tooltip for collapsed state */}
              {!isOpen && (
                <div className="absolute left-full top-1/2 -translate-y-1/2 ml-4 px-2 py-1 bg-slate-800 text-xs text-white rounded opacity-0 group-hover:opacity-100 pointer-events-none transition-opacity whitespace-nowrap z-50 border border-white/10">
                  {item.label}
                </div>
              )}
            </button>
          );
        })}
      </nav>

      {/* User & Logout - Quick Switcher */}
      <div className="p-4 border-t border-white/5 relative" ref={profileMenuRef}>
        
        {/* Profile Menu Dropdown */}
        {isProfileMenuOpen && (
          <div className={`absolute bottom-full left-4 right-4 mb-2 bg-[#1e293b] border border-white/10 rounded-xl shadow-2xl overflow-hidden animate-slide-up z-50 ${!isOpen ? 'left-16 w-56 bottom-0' : ''}`}>
             <div className="p-2 space-y-1">
                <div className="px-2 py-1.5 text-xs font-semibold text-slate-500 uppercase tracking-wider">{t('switch_profile')}</div>
                <div className="h-px bg-white/5 my-1" />
                {profiles.map(profile => (
                  <button
                    key={profile.id}
                    onClick={() => {
                      if (profile.id !== currentUser?.id) {
                         onSwitchProfile(profile);
                      }
                      setIsProfileMenuOpen(false);
                    }}
                    className={`w-full flex items-center px-2 py-2 rounded-lg transition-colors ${profile.id === currentUser?.id ? 'bg-indigo-600/20 text-indigo-300' : 'hover:bg-white/5 text-slate-300 hover:text-white'}`}
                  >
                    <div className={`w-6 h-6 rounded-md flex items-center justify-center mr-3 overflow-hidden ${profile.avatarColor}`}>
                      {profile.avatarImage ? (
                        <img src={profile.avatarImage} alt={profile.name} className="w-full h-full object-cover" loading="lazy" />
                      ) : (
                        <span className="text-xs font-bold text-white">{profile.name[0]}</span>
                      )}
                    </div>
                    <span className="text-sm font-medium flex-1 text-left">{profile.name}</span>
                    {profile.id === currentUser?.id && <Check size={14} />}
                  </button>
                ))}
                <div className="h-px bg-white/5 my-1" />
                <button 
                  onClick={() => {
                    onLogout();
                    setIsProfileMenuOpen(false);
                  }}
                  className="w-full flex items-center px-2 py-2 rounded-lg text-red-400 hover:bg-red-500/10 transition-colors text-sm"
                >
                   <LogOut size={16} className="mr-3" />
                   {t('logout')}
                </button>
             </div>
          </div>
        )}

        {/* User Button */}
        <button 
          onClick={() => setIsProfileMenuOpen(!isProfileMenuOpen)}
          className={`w-full flex items-center p-2 rounded-xl border border-transparent hover:bg-slate-800/50 hover:border-white/5 transition-all duration-300 ${!isOpen ? 'justify-center' : ''} ${isProfileMenuOpen ? 'bg-slate-800 border-white/5' : ''}`}
        >
          <div className={`w-8 h-8 rounded-full flex items-center justify-center shrink-0 shadow-lg overflow-hidden ${currentUser?.avatarColor || 'bg-slate-700'}`}>
            {currentUser?.avatarImage ? (
              <img src={currentUser.avatarImage} alt={currentUser.name} className="w-full h-full object-cover" loading="lazy" />
            ) : (
              <span className="font-bold text-white text-xs">{currentUser?.name?.[0] || 'U'}</span>
            )}
          </div>
          
          <div className={`ml-3 overflow-hidden transition-all duration-300 text-left flex-1 ${isOpen ? 'w-auto opacity-100' : 'w-0 opacity-0 hidden'}`}>
            <p className="text-sm font-bold text-white truncate">{currentUser?.name || t('user')}</p>
            <p className="text-[10px] text-slate-400 truncate">{currentUser?.isKid ? t('kid_profile') : t('standard_profile')}</p>
          </div>
          
          {isOpen && (
             <ChevronUp size={16} className={`text-slate-500 transition-transform duration-300 ${isProfileMenuOpen ? 'rotate-180' : ''}`} />
          )}
        </button>
      </div>
    </aside>

    {/* Mobile header + bottom navigation */}
    {isMobile ? (
      <>
        <div className="fixed top-0 left-0 right-0 z-50 backdrop-blur-md bg-black/10 border-b border-white/5">
          <div className="h-12 flex items-center justify-between px-4">
            <div className="flex items-center gap-2">
              <div className="text-lg font-bold tracking-tight bg-clip-text text-transparent bg-gradient-to-r from-indigo-500 to-purple-600">
                ArcaNea
              </div>
              {scanRunning ? (
                <div className="text-[11px] text-slate-300 bg-white/5 border border-white/10 rounded-full px-2 py-0.5">
                  {typeof scanProgress === 'number' ? `${scanProgress}%` : '...'}
                </div>
              ) : null}
            </div>
            <button
              type="button"
              onClick={() => {
                requestFocusSearchRef.current = true;
                onRequestOpen?.();
              }}
              className="p-2 rounded-lg text-slate-300 hover:text-white hover:bg-white/10 transition-colors"
              aria-label={t('search')}
            >
              <Search size={18} />
            </button>
          </div>
        </div>

        {isOpen ? (
          <>
            <div className="fixed inset-0 z-40" onClick={() => onRequestClose?.()} aria-hidden="true" />
            <div className="fixed top-12 left-0 right-0 z-50 px-4 pt-3 pb-4 bg-[#0b1220]/95 backdrop-blur-xl border-b border-white/5">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400 pointer-events-none" size={16} />
                <input 
                  ref={searchInputRef}
                  type="text" 
                  placeholder={t('search_placeholder')} 
                  value={searchQuery}
                  onChange={(e) => onSearch(e.target.value)}
                  onBlur={() => {
                    if (!pinned && onRequestClose) {
                      onRequestClose();
                    }
                  }}
                  onKeyDown={(e) => {
                    if (e.key === 'Escape') {
                      e.preventDefault();
                      onRequestClose?.();
                    }
                  }}
                  className="w-full bg-slate-800/50 border border-white/5 rounded-lg py-2 pl-10 pr-8 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:ring-1 focus:ring-indigo-500/50 transition-all"
                />
                {searchQuery && (
                  <button 
                    onClick={() => {
                      onSearch('');
                      searchInputRef.current?.focus();
                    }}
                    className="absolute right-2 top-1/2 -translate-y-1/2 text-slate-500 hover:text-white p-1 rounded-full hover:bg-white/10 transition-colors"
                  >
                    <X size={14} />
                  </button>
                )}
              </div>
            </div>
          </>
        ) : null}

        {isProfileMenuOpen && (
          <div className="fixed inset-0 z-40" onClick={() => setIsProfileMenuOpen(false)} aria-hidden="true" />
        )}
        {isProfileMenuOpen && (
          <div ref={profileMenuRef} className="fixed bottom-20 left-4 right-4 z-50 bg-[#1e293b] border border-white/10 rounded-xl shadow-2xl overflow-hidden animate-slide-up">
            <div className="p-2 space-y-1">
              <div className="px-2 py-1.5 text-xs font-semibold text-slate-500 uppercase tracking-wider">{t('switch_profile')}</div>
              <div className="h-px bg-white/5 my-1" />
              {profiles.map(profile => (
                <button
                  key={profile.id}
                  onClick={() => {
                    if (profile.id !== currentUser?.id) {
                      onSwitchProfile(profile);
                    }
                    setIsProfileMenuOpen(false);
                  }}
                  className={`w-full flex items-center px-2 py-2 rounded-lg transition-colors ${profile.id === currentUser?.id ? 'bg-indigo-600/20 text-indigo-300' : 'hover:bg-white/5 text-slate-300 hover:text-white'}`}
                >
                  <div className={`w-6 h-6 rounded-md flex items-center justify-center mr-3 overflow-hidden ${profile.avatarColor}`}>
                    {profile.avatarImage ? (
                      <img src={profile.avatarImage} alt={profile.name} className="w-full h-full object-cover" loading="lazy" />
                    ) : (
                      <span className="text-xs font-bold text-white">{profile.name[0]}</span>
                    )}
                  </div>
                  <span className="text-sm font-medium flex-1 text-left">{profile.name}</span>
                  {profile.id === currentUser?.id && <Check size={14} />}
                </button>
              ))}
              <div className="h-px bg-white/5 my-1" />
              <button 
                onClick={() => {
                  onLogout();
                  setIsProfileMenuOpen(false);
                }}
                className="w-full flex items-center px-2 py-2 rounded-lg text-red-400 hover:bg-red-500/10 transition-colors text-sm"
              >
                 <LogOut size={16} className="mr-3" />
                 {t('logout')}
              </button>
            </div>
          </div>
        )}
        <nav className="fixed bottom-0 left-0 right-0 z-50 bg-[#0b1220]/95 backdrop-blur-xl border-t border-white/5 pb-[env(safe-area-inset-bottom)]">
          <div className="h-14 flex items-center justify-around px-2">
            {[
              { id: 'home', label: t('nav_home'), icon: Home },
              { id: 'movies', label: t('nav_movies'), icon: Film },
              { id: 'tv', label: t('nav_series'), icon: Tv },
            ].map((item) => {
              const Icon = item.icon;
              const active = currentView === item.id;
              return (
                <button
                  key={item.id}
                  onClick={() => {
                    onChangeView(item.id as ViewState);
                    setIsProfileMenuOpen(false);
                  }}
                  className={`flex flex-col items-center justify-center text-xs transition-transform active:scale-110 ${active ? 'text-indigo-500' : 'text-slate-400 hover:text-white'}`}
                >
                  <Icon size={20} className={active ? 'text-indigo-500' : 'text-slate-400'} />
                  <div className={`mt-1 h-1 w-1 rounded-full ${active ? 'bg-indigo-500 shadow-[0_0_8px_rgba(99,102,241,0.8)]' : 'bg-transparent'}`} />
                  <span className="mt-1">{item.label}</span>
                </button>
              );
            })}
            <button
              onClick={() => setIsProfileMenuOpen(!isProfileMenuOpen)}
              className="flex flex-col items-center justify-center text-xs text-slate-400 hover:text-white transition-transform active:scale-110"
            >
              <User size={20} />
              <div className="mt-1 h-1 w-1 rounded-full bg-transparent" />
              <span className="mt-1">{t('profile')}</span>
            </button>
          </div>
        </nav>
      </>
    ) : null}
    </>
  );
};
