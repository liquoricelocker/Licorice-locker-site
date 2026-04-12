/**
 * Session-based storefront analytics (funnel, time-on-page, affiliate ref in localStorage).
 * Admin views under /dashboard/* are not served this file.
 */
(function () {
  var SESSION_KEY = 'll_analytics_session_id';
  var AFF_KEY = 'll_analytics_affiliate_ref';
  var sessionId = null;
  var readyChain = Promise.resolve();

  function getRef() {
    try {
      var q = new URLSearchParams(window.location.search).get('ref');
      if (q) {
        localStorage.setItem(AFF_KEY, q);
        return q;
      }
      return localStorage.getItem(AFF_KEY) || null;
    } catch (e) {
      return null;
    }
  }

  function postJSON(url, body) {
    return fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      credentials: 'same-origin',
      keepalive: true,
    });
  }

  function sendBeaconJSON(url, body) {
    try {
      if (navigator.sendBeacon) {
        var blob = new Blob([JSON.stringify(body)], { type: 'application/json' });
        return navigator.sendBeacon(url, blob);
      }
    } catch (e) {}
    return postJSON(url, body);
  }

  function ensureSession() {
    if (sessionId) return Promise.resolve(sessionId);
    try {
      sessionId = localStorage.getItem(SESSION_KEY);
    } catch (e) {}
    var affiliate = getRef();
    var payload = { affiliate: affiliate, session_id: sessionId || null };
    return fetch('/api/analytics/session/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      credentials: 'same-origin',
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (d) {
        sessionId = d.session_id;
        try {
          localStorage.setItem(SESSION_KEY, sessionId);
        } catch (e) {}
        return sessionId;
      });
  }

  function track(event, meta) {
    meta = meta || {};
    readyChain = readyChain
      .then(function () {
        return ensureSession();
      })
      .then(function (sid) {
        var body = {
          session_id: sid,
          event: event,
          page: window.location.pathname + window.location.search,
          meta: meta,
        };
        if (event === 'time_on_page') {
          sendBeaconJSON('/api/analytics/track', body);
          return;
        }
        return postJSON('/api/analytics/track', body);
      })
      .catch(function () {});
    return readyChain;
  }

  function convert() {
    readyChain = readyChain
      .then(function () {
        return ensureSession();
      })
      .then(function (sid) {
        return postJSON('/api/analytics/convert', { session_id: sid });
      })
      .catch(function () {});
    return readyChain;
  }

  window.LLAnalytics = { track: track, convert: convert, ensureSession: ensureSession };

  var ofetch = window.fetch;
  window.fetch = function (input, init) {
    var url = typeof input === 'string' ? input : input && input.url ? input.url : '';
    return ofetch.apply(this, arguments).then(function (res) {
      if (url.indexOf('/cart/add') !== -1 && res.ok) {
        res
          .clone()
          .json()
          .then(function (data) {
            if (!data || !data.ok) return;
            var pid = null;
            try {
              if (init && init.body instanceof FormData && init.body.get) {
                pid = init.body.get('product_id');
              }
            } catch (e) {}
            window.LLAnalytics.track('add_to_cart', {
              productId: pid != null ? String(pid) : null,
            });
          })
          .catch(function () {});
      }
      return res;
    });
  };

  document.addEventListener(
    'submit',
    function (e) {
      var f = e.target;
      if (!f || !f.getAttribute || !f.action) return;
      if (f.action.indexOf('/cart/add') === -1) return;
      var pidEl = f.querySelector('[name="product_id"]');
      var pid = pidEl ? pidEl.value : null;
      window.LLAnalytics.track('add_to_cart', { productId: pid, via: 'form' });
    },
    true
  );

  ensureSession()
    .then(function () {
      track('page_view', { title: document.title });
      var p = window.location.pathname;
      if (p.indexOf('/product/') === 0) {
        var slug = p.replace(/^\/product\//, '').split('/')[0];
        track('view_product', { slug: slug || null });
      }
      if (p === '/cart') {
        track('view_cart', {});
      }
      if (p.indexOf('/checkout') !== -1) {
        track('start_checkout', {});
      }
    })
    .catch(function () {});

  var startTime = Date.now();
  window.addEventListener('beforeunload', function () {
    var dur = Date.now() - startTime;
    var sid = sessionId;
    try {
      if (!sid) sid = localStorage.getItem(SESSION_KEY);
    } catch (e) {}
    if (!sid) return;
    sendBeaconJSON('/api/analytics/track', {
      session_id: sid,
      event: 'time_on_page',
      page: window.location.pathname,
      meta: { duration_ms: dur },
    });
  });

  document.addEventListener(
    'click',
    function (e) {
      var a = e.target.closest && e.target.closest('a[href*="checkout"]');
      if (!a) return;
      window.LLAnalytics.track('start_checkout', { via: 'link' });
    },
    true
  );

  function wireCheckout() {
    var form = document.querySelector('form.js-checkout-form');
    if (!form) return;
    var shipNames = [
      'shipping_line1',
      'shipping_line2',
      'shipping_city',
      'shipping_region',
      'shipping_postal',
      'shipping_country',
    ];
    var shipOnce = false;
    shipNames.forEach(function (nm) {
      var el = form.querySelector('[name="' + nm + '"]');
      if (!el) return;
      var t0 = 0;
      el.addEventListener('focus', function () {
        t0 = Date.now();
        if (!shipOnce) {
          shipOnce = true;
          window.LLAnalytics.track('enter_shipping', { field: nm });
        }
      });
      el.addEventListener('blur', function () {
        var ms = Date.now() - t0;
        if (ms >= 8000) {
          window.LLAnalytics.track('hesitation', {
            step: 'shipping',
            field: nm,
            timeSpent: ms,
          });
        }
      });
    });
    var btn = form.querySelector('.js-checkout-submit');
    if (btn) {
      btn.addEventListener(
        'mousedown',
        function () {
          window.LLAnalytics.track('enter_payment', {});
        },
        { once: true }
      );
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', wireCheckout);
  } else {
    wireCheckout();
  }
})();
