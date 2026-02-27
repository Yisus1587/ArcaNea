/* ArcaNea PWA Service Worker (local-first UI shell; network-first API) */
// Bump this when shipping a new build to avoid stale `index.html` referencing removed hashed assets.
const CACHE_NAME = 'arcanea-pwa-v2';
const PRECACHE_URLS = [
  '/index.html',
  '/manifest.webmanifest',
  '/icons/arcanea-icon.svg',
  '/icons/arcanea-maskable.svg'
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    (async () => {
      try {
        const cache = await caches.open(CACHE_NAME);
        await cache.addAll(PRECACHE_URLS);
      } catch (e) {
        // best-effort
      }
      self.skipWaiting();
    })()
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    (async () => {
      try {
        const keys = await caches.keys();
        await Promise.all(keys.map((k) => (k !== CACHE_NAME ? caches.delete(k) : Promise.resolve())));
      } catch (e) {
        // ignore
      }
      self.clients.claim();
    })()
  );
});

const isSameOrigin = (url) => {
  try {
    return url.origin === self.location.origin;
  } catch {
    return false;
  }
};

const networkFirst = async (request, fetchInit) => {
  const cache = await caches.open(CACHE_NAME);
  try {
    const response = await fetch(request, fetchInit);
    if (response && response.ok) {
      try {
        await cache.put(request, response.clone());
      } catch {
        // ignore
      }
    }
    return response;
  } catch (e) {
    const cached = await cache.match(request);
    if (cached) return cached;
    throw e;
  }
};

const staleWhileRevalidate = async (request) => {
  const cache = await caches.open(CACHE_NAME);
  const cached = await cache.match(request);
  const fetchPromise = (async () => {
    try {
      const response = await fetch(request);
      if (response && response.ok) {
        try {
          await cache.put(request, response.clone());
        } catch {
          // ignore
        }
      }
      return response;
    } catch (e) {
      return null;
    }
  })();

  return cached || (await fetchPromise) || new Response('Offline', { status: 503 });
};

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);
  if (!isSameOrigin(url)) return;

  // API: network-first, avoid stale data
  if (url.pathname.startsWith('/api/')) {
    event.respondWith(networkFirst(req));
    return;
  }

  // SPA navigation: network-first, fallback to cached shell
  if (req.mode === 'navigate') {
    event.respondWith(
      (async () => {
        try {
          // Avoid serving a stale app shell from HTTP cache; prevents hashed asset 404s after rebuilds.
          return await networkFirst(req, { cache: 'no-store' });
        } catch (e) {
          const cache = await caches.open(CACHE_NAME);
          return (await cache.match('/index.html')) || (await cache.match('/')) || new Response('Offline', { status: 503 });
        }
      })()
    );
    return;
  }

  // Static assets: stale-while-revalidate
  event.respondWith(staleWhileRevalidate(req));
});
