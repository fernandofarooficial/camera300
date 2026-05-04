const CACHE_NAME = 'checkin-m-v1';

const PRECACHE = [
  '/camera300/m/',
  '/camera300/m/tracks/lista',
  '/camera300/m/tracks/tabuleiro',
  '/camera300/m/tracks/caixa',
  '/camera300/m/tracks/quadro',
];

// Rotas que NUNCA devem ser interceptadas (tempo-real e APIs)
const BYPASS = [
  '/stream',
  '/api/',
  '/camera300/tracks/api/',
  '/camera300/tracks/caixa/nf/',
  '/camera300/tracks/snapshot/',
];

function shouldBypass(url) {
  return BYPASS.some(p => url.includes(p));
}

// ── Install ──────────────────────────────────────────────────────────────────
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => cache.addAll(PRECACHE))
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

// ── Mensagens da página (ex.: SKIP_WAITING) ──────────────────────────────────
self.addEventListener('message', event => {
  if (event.data && event.data.type === 'SKIP_WAITING') self.skipWaiting();
});

// ── Fetch: network-first; cache só como fallback offline ─────────────────────
self.addEventListener('fetch', event => {
  const { request } = event;

  if (request.method !== 'GET' || shouldBypass(request.url)) return;

  event.respondWith(
    fetch(request)
      .then(response => {
        if (response && response.status === 200 && response.type === 'basic') {
          const clone = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(request, clone));
        }
        return response;
      })
      .catch(() => caches.match(request))
  );
});
