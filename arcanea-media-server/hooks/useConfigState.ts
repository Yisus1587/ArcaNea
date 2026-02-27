import { useState } from 'react';
import { MetadataConfig } from '../types';

export const useConfigState = (defaultProviderLanguage: string) => {
  const [metadataConfig, setMetadataConfig] = useState<MetadataConfig>({
    moviesProvider: 'tmdb',
    animeProvider: 'jikan',
    language: defaultProviderLanguage,
    downloadImages: true,
    fetchCast: true,
  });
  const [libraryPaths, setLibraryPaths] = useState<string[]>([]);

  return {
    metadataConfig,
    setMetadataConfig,
    libraryPaths,
    setLibraryPaths,
  };
};
