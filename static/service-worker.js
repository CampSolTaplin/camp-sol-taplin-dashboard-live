var CACHE_NAME = 'cst-staff-v2';
var STATIC_ASSETS = [
  '/static/css/dashboard.css',
  '/static/css/attendance.css',
  '/static/js/dashboard.js',
  '/static/js/attendance.js',
  '/static/js/push-notifications.js',
  '/static/images/logo.png',
  '/static/images/icon-192.png',
  '/static/images/icon-512.png'
];

// Install: pre-cache static assets
self.addEventListener('install', function(event) {
  event.waitUntil(
    caches.open(CACHE_NAME).then(function(cache) {
      return cache.addAll(STATIC_ASSETS);
    })
  );
  self.skipWaiting();
});

// Activate: clean old caches
self.addEventListener('activate', function(event) {
  event.waitUntil(
    caches.keys().then(function(cacheNames) {
      return Promise.all(
        cacheNames
          .filter(function(name) { return name !== CACHE_NAME; })
          .map(function(name) { return caches.delete(name); })
      );
    })
  );
  self.clients.claim();
});

// Fetch: cache-first for static, network-first for API/HTML
self.addEventListener('fetch', function(event) {
  var url = new URL(event.request.url);

  // Skip non-GET requests
  if (event.request.method !== 'GET') return;

  // Skip cross-origin requests (e.g. Google Fonts CDN)
  if (url.origin !== self.location.origin) return;

  // Static assets: cache-first
  if (url.pathname.startsWith('/static/')) {
    event.respondWith(
      caches.match(event.request).then(function(cached) {
        return cached || fetch(event.request).then(function(response) {
          var clone = response.clone();
          caches.open(CACHE_NAME).then(function(cache) {
            cache.put(event.request, clone);
          });
          return response;
        });
      })
    );
    return;
  }

  // API calls and HTML pages: network-first with cache fallback
  event.respondWith(
    fetch(event.request).then(function(response) {
      // Only cache successful responses
      if (response.ok) {
        var clone = response.clone();
        caches.open(CACHE_NAME).then(function(cache) {
          cache.put(event.request, clone);
        });
      }
      return response;
    }).catch(function() {
      return caches.match(event.request);
    })
  );
});

// Push notification received
self.addEventListener('push', function(event) {
  var data = { title: 'Camp Sol Taplin', body: 'New notification', url: '/' };
  if (event.data) {
    try {
      data = Object.assign(data, event.data.json());
    } catch (e) {
      data.body = event.data.text();
    }
  }

  event.waitUntil(
    self.registration.showNotification(data.title, {
      body: data.body,
      icon: '/static/images/icon-192.png',
      badge: '/static/images/icon-192.png',
      data: { url: data.url || '/' },
      vibrate: [200, 100, 200]
    })
  );
});

// Notification click: open/focus the app
self.addEventListener('notificationclick', function(event) {
  event.notification.close();
  var targetUrl = event.notification.data && event.notification.data.url ? event.notification.data.url : '/';

  event.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then(function(clients) {
      // Try to focus an existing window
      for (var i = 0; i < clients.length; i++) {
        if (clients[i].url.indexOf(self.location.origin) !== -1 && 'focus' in clients[i]) {
          clients[i].navigate(targetUrl);
          return clients[i].focus();
        }
      }
      // Open a new window
      return self.clients.openWindow(targetUrl);
    })
  );
});
