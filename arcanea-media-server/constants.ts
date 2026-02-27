import { MediaItem } from './types';

// Fallback mock data to demonstrate UI when backend is offline
export const MOCK_MEDIA: MediaItem[] = [
  {
    id: '1',
    title: 'Interstellar',
    path: '/media/movies/interstellar.mkv',
    type: 'movie',
    year: 2014,
    duration: 10140,
    thumbnailUrl: 'https://picsum.photos/seed/interstellar/400/600',
    backdropUrl: 'https://picsum.photos/seed/interstellar-bg/1920/1080',
    overview: 'A team of explorers travel through a wormhole in space in an attempt to ensure humanity\'s survival.',
    rating: 8.6,
    genre: ['Sci-Fi', 'Adventure'],
    addedAt: '2023-10-01T10:00:00Z'
  },
  {
    id: '2',
    title: 'Cyberpunk: Edgerunners',
    path: '/media/anime/cyberpunk.mkv',
    type: 'anime',
    year: 2022,
    duration: 1400,
    thumbnailUrl: 'https://picsum.photos/seed/cyberpunk/400/600',
    backdropUrl: 'https://picsum.photos/seed/cyberpunk-bg/1920/1080',
    overview: 'A street kid tries to survive in a technology and body modification-obsessed city of the future.',
    rating: 9.1,
    genre: ['Action', 'Sci-Fi'],
    addedAt: '2023-10-05T12:00:00Z'
  },
  {
    id: '3',
    title: 'The Matrix',
    path: '/media/movies/matrix.mkv',
    type: 'movie',
    year: 1999,
    duration: 8160,
    thumbnailUrl: 'https://picsum.photos/seed/matrix/400/600',
    backdropUrl: 'https://picsum.photos/seed/matrix-bg/1920/1080',
    overview: 'A computer hacker learns from mysterious rebels about the true nature of his reality and his role in the war against its controllers.',
    rating: 8.7,
    genre: ['Action', 'Sci-Fi'],
    addedAt: '2023-09-15T10:00:00Z'
  },
  {
    id: '4',
    title: 'Breaking Bad',
    path: '/media/tv/bb.mkv',
    type: 'series',
    year: 2008,
    duration: 3000,
    thumbnailUrl: 'https://picsum.photos/seed/bb/400/600',
    backdropUrl: 'https://picsum.photos/seed/bb-bg/1920/1080',
    overview: 'A high school chemistry teacher turned methamphetamine manufacturing drug dealer.',
    rating: 9.5,
    genre: ['Crime', 'Drama'],
    addedAt: '2023-08-01T10:00:00Z'
  },
  {
    id: '5',
    title: 'Inception',
    path: '/media/movies/inception.mkv',
    type: 'movie',
    year: 2010,
    duration: 8880,
    thumbnailUrl: 'https://picsum.photos/seed/inception/400/600',
    backdropUrl: 'https://picsum.photos/seed/inception-bg/1920/1080',
    overview: 'A thief who steals corporate secrets through the use of dream-sharing technology is given the inverse task of planting an idea into the mind of a C.E.O.',
    rating: 8.8,
    genre: ['Action', 'Sci-Fi'],
    addedAt: '2023-10-10T10:00:00Z'
  },
   {
    id: '6',
    title: 'Dune: Part Two',
    path: '/media/movies/dune2.mkv',
    type: 'movie',
    year: 2024,
    duration: 9960,
    thumbnailUrl: 'https://picsum.photos/seed/dune/400/600',
    backdropUrl: 'https://picsum.photos/seed/dune-bg/1920/1080',
    overview: 'Paul Atreides unites with Chani and the Fremen while on a warpath of revenge against the conspirators who destroyed his family.',
    rating: 9.0,
    genre: ['Sci-Fi', 'Adventure'],
    addedAt: '2023-11-01T10:00:00Z'
  }
];
