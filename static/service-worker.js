const CACHE_NAME = 'checkin-v1';

// Apenas recursos estáticos que valem a pena guardar em cache
const PRECACHE = [
  '/',
];

// URLs que NUNCA devem ser interceptadas (tempo-real e APIs)
const BYPASS = [
  '/stream',
  '/api/',
  '/camera300/tracks/api/',
];

function shouldBypass(url) {
  return BYPASS.some(p => url.includes(p));
}

// ── Install ──────────────────────────────────────────────────────────────────
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => cache.addAll(PRECACHE))
      .then(() => self.skipWaiting())
  );
});

// ── Activate: limpa caches antigos ──────────────────────────────────────────
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k))
      )
    ).then(() => self.clients.claim())
  );
});

// ── Fetch: network-first; cache só como fallback offline ─────────────────────
self.addEventListener('fetch', event => {
  const { request } = event;

  // Ignora requisições não-GET e rotas em tempo real / API
  if (request.method !== 'GET' || shouldBypass(request.url)) return;

  event.respondWith(
    fetch(request)
      .then(response => {
        // Armazena cópia no cache apenas para respostas válidas
        if (response && response.status === 200 && response.type === 'basic') {
          const clone = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(request, clone));
        }
        return response;
      })
      .catch(() => caches.match(request))
  );
});
