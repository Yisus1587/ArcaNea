export interface MediaItem {
  id: string;
  title: string;
  path: string;
  type: 'movie' | 'series' | 'anime';
  year?: number;
  duration?: number; // in minutes (legacy field; may be unset)
  thumbnailUrl?: string;
  backdropUrl?: string;
  overview?: string;
  rating?: number;
  genre?: string[];
  // New quick-access fields populated from backend/enrichment
  malId?: string | number;
  posterPath?: string; // backend local poster path or URL
  isAnimated?: boolean;
  origin?: string; // e.g., 'JP', 'US'
  releaseYear?: number;
  runtime?: number; // in minutes
  cast?: string[];
  addedAt: string;
  files?: Array<{ filename?: string; path?: string; size?: number; mtime?: number; index?: number }>;
  rawMetadata?: any;
}

export interface ScanStatus {
  // Backend may provide total/processed counts and current item; make them optional
  total?: number;
  processed?: number;
  scanning: boolean;
  progress: number;
  currentFile?: string;
  // alternate names some backends use
  current?: string;
  totalFiles?: number;
  processedFiles?: number;
  status?: string;
  // enrichment status returned from backend (optional)
  enrichment?: EnrichmentState;
}

export interface EnrichmentState {
  running: boolean;
  current_id?: number | string | null;
  current_title?: string | null;
  current_step?: string | null;
  last_updated?: number | null;
  // DB ingestion counts
  total?: number;
  pending?: number;
  enriched?: number;
}

export interface Drive {
  path: string;
  label: string;
  totalSpace: number;
  freeSpace: number;
}

export interface MetadataConfig {
  moviesProvider: 'tmdb' | 'tvdb' | 'omdb';
  animeProvider: 'jikan' | 'anilist' | 'kitsu';
  language: string;
  downloadImages: boolean;
  fetchCast: boolean;
}

export interface UserProfile {
  id: string;
  name: string;
  avatarColor: string; // Tailwind class like 'bg-indigo-500'
  avatarImage?: string; // data URL (optional)
  isKid: boolean;
  // Perfil de gestión: puede acceder a Ajustes/administración del servidor
  isManager?: boolean;
  // PIN corto y opcional (recomendado 4–6 dígitos)
  pin?: string;
  language: string;
}

export interface Notification {
  id: string;
  type: 'success' | 'error' | 'info';
  message: string;
  durationMs?: number;
  variant?: 'default' | 'welcome';
}

export type AppStage = 'booting' | 'setup' | 'profile_select' | 'app';
export type ViewState = 'home' | 'movies' | 'tv' | 'list' | 'settings';
