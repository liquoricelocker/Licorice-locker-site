/**
 * AJAX add-to-cart + Mini Series upsell modal (one prompt per add).
 */
(function () {
  function updateHeaderCart(n) {
    var link = document.querySelector('.header-cart');
    if (!link) return;
    n = parseInt(n, 10) || 0;
    if (n > 0) {
      link.setAttribute('aria-label', 'Shopping cart, ' + n + ' items');
    } else {
      link.setAttribute('aria-label', 'Shopping cart');
    }
    var inner = link.querySelector('.header-cart-inner');
    var heart = link.querySelector('.header-cart-heart');
    if (n > 0) {
      if (!heart && inner) {
        heart = document.createElement('span');
        heart.className = 'header-cart-heart';
        heart.setAttribute('aria-hidden', 'true');
        heart.textContent = '\u2764';
        inner.insertBefore(heart, inner.firstChild);
      }
    } else if (heart) {
      heart.remove();
    }
  }

  function showFlashOk(msg) {
    var main = document.querySelector('main.main');
    if (!main) return;
    var wrap = document.querySelector('.flash-wrap');
    if (!wrap) {
      wrap = document.createElement('div');
      wrap.className = 'flash-wrap';
      main.insertBefore(wrap, main.firstChild);
    }
    var p = document.createElement('p');
    p.className = 'flash flash-ok';
    p.textContent = msg;
    wrap.appendChild(p);
    setTimeout(function () {
      p.remove();
      if (wrap && !wrap.children.length) {
        wrap.remove();
      }
    }, 4000);
  }

  function cartAddAction(form) {
    var action = (form.getAttribute('action') || '').toLowerCase();
    return action.indexOf('/cart/add') !== -1;
  }

  function openUpsellModal(payload) {
    var modal = document.getElementById('cart-upsell-modal');
    if (!modal || !payload) return;
    var title = modal.querySelector('[data-upsell-title]');
    var body = modal.querySelector('[data-upsell-body]');
    var img = modal.querySelector('[data-upsell-img]');
    var primary = modal.querySelector('[data-upsell-primary]');
    var secondary = modal.querySelector('[data-upsell-secondary]');
    if (title) title.textContent = payload.title || '';
    if (body) body.textContent = payload.body || '';
    if (img) {
      if (payload.image) {
        img.src = payload.image;
        img.alt = '';
        img.hidden = false;
      } else {
        img.removeAttribute('src');
        img.hidden = true;
      }
    }
    if (primary) {
      primary.textContent = payload.primary_label || 'Continue';
      primary.href = payload.primary_href || '#';
    }
    if (secondary) {
      secondary.textContent = payload.secondary_label || 'Continue';
      secondary.onclick = null;
      var sh = payload.secondary_href;
      if (sh) {
        secondary.setAttribute('href', sh);
        secondary.classList.remove('cart-upsell-secondary--muted');
      } else {
        secondary.removeAttribute('href');
        secondary.classList.add('cart-upsell-secondary--muted');
        secondary.onclick = function (ev) {
          ev.preventDefault();
          closeUpsellModal();
        };
      }
    }
    modal.setAttribute('aria-hidden', 'false');
    document.body.classList.add('cart-upsell-modal-open');
    if (primary) primary.focus();
  }

  function closeUpsellModal() {
    var modal = document.getElementById('cart-upsell-modal');
    if (!modal) return;
    modal.setAttribute('aria-hidden', 'true');
    document.body.classList.remove('cart-upsell-modal-open');
  }

  document.addEventListener('click', function (e) {
    var t = e.target;
    if (!t || !t.closest) return;
    var closer = t.closest('[data-cart-upsell-close]');
    if (closer) {
      e.preventDefault();
      closeUpsellModal();
    }
  });

  document.addEventListener('keydown', function (e) {
    if (e.key !== 'Escape' || !document.body.classList.contains('cart-upsell-modal-open')) return;
    if (document.body.classList.contains('affiliate-modal-open')) return;
    if (document.body.classList.contains('affiliate-terms-modal-open')) return;
    if (document.body.classList.contains('slide-menu-open')) return;
    closeUpsellModal();
  });

  document.addEventListener('submit', function (e) {
    var form = e.target;
    if (!form || form.tagName !== 'FORM' || !cartAddAction(form)) return;
    if (form.getAttribute('data-no-ajax') === '1') return;
    e.preventDefault();
    var fd = new FormData(form);
    fd.set('ajax', '1');
    fetch(form.action, {
      method: 'POST',
      body: fd,
      credentials: 'same-origin',
      headers: { 'X-Requested-With': 'XMLHttpRequest' },
    })
      .then(function (res) {
        return res.json().then(function (data) {
          return { ok: res.ok, data: data };
        });
      })
      .then(function (result) {
        if (result.ok && result.data && result.data.ok) {
          updateHeaderCart(result.data.cart_item_count);
          if (result.data.upsell) {
            openUpsellModal(result.data.upsell);
          } else {
            showFlashOk(result.data.message || 'Added to cart.');
          }
        } else {
          var m = (result.data && result.data.message) || 'Could not add to cart.';
          window.alert(m);
        }
      })
      .catch(function () {
        window.alert('Something went wrong. Please try again.');
      });
  });
})();
