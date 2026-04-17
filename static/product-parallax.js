/**
 * Micro-parallax on product hero images (shop + Listening Room).
 * translateY only, clamped; passive scroll; rAF smoothing; respects reduced motion.
 */
(function () {
  "use strict";

  if (typeof window === "undefined" || !window.document) return;
  if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;

  var SELECTOR = [
    ".page-shop .soundwave-banner-img",
    ".page-shop .riff-banner-img",
    ".page-shop .harmony-banner-img",
    ".page-shop .melody-banner-img",
    ".page-shop .allegro-banner-img",
    ".page-shop .link-series-promo-img",
  ].join(",");

  var LERP = 0.13;
  var EPS = 0.02;
  var OUT_PAD = 120;

  function maxShiftPx() {
    /* Shop 18px → 25.2px; Listening Room 26px → 36.4px (+40%); mobile still 50% of base */
    var base = document.body.classList.contains("page-listening-room") ? 36.4 : 25.2;
    if (window.matchMedia("(max-width: 768px)").matches) base *= 0.5;
    return base;
  }

  function ensureClip(img) {
    if (img.dataset.parallaxClip) return;
    var p = img.parentElement;
    if (p && (p.tagName === "A" || (p.classList && p.classList.contains("link-series-promo-inner")))) {
      p.style.overflow = "hidden";
    }
    img.dataset.parallaxClip = "1";
  }

  function targetY(img, rect, maxPx) {
    var vh = window.innerHeight || 1;
    if (rect.height < 2) return 0;
    var mid = rect.top + rect.height * 0.5;
    var vmid = vh * 0.5;
    var n = (mid - vmid) / (vh * 0.55);
    if (n > 1) n = 1;
    if (n < -1) n = -1;
    return -n * maxPx;
  }

  var imgs = [];
  var rafId = 0;

  function collect() {
    imgs = Array.prototype.slice.call(document.querySelectorAll(SELECTOR));
    for (var i = 0; i < imgs.length; i++) ensureClip(imgs[i]);
  }

  function step() {
    rafId = 0;
    var maxPx = maxShiftPx();
    var moving = false;
    var vh = window.innerHeight;

    for (var i = 0; i < imgs.length; i++) {
      var img = imgs[i];
      var rect = img.getBoundingClientRect();
      var out = rect.bottom < -OUT_PAD || rect.top > vh + OUT_PAD;
      var tgt = out ? 0 : targetY(img, rect, maxPx);
      var cur = typeof img._parallaxY === "number" ? img._parallaxY : 0;
      var next = cur + (tgt - cur) * LERP;
      if (Math.abs(next - tgt) < EPS) next = tgt;
      if (Math.abs(next - tgt) > 0.02) moving = true;
      img._parallaxY = next;

      if (out && Math.abs(next) < EPS) {
        img._parallaxY = 0;
        img.style.transform = "";
        img.style.willChange = "auto";
        continue;
      }

      img.style.willChange = "transform";
      img.style.transform = "translate3d(0, " + next.toFixed(2) + "px, 0)";
    }

    if (moving) rafId = window.requestAnimationFrame(step);
  }

  function kick() {
    if (!rafId) rafId = window.requestAnimationFrame(step);
  }

  function init() {
    collect();
    kick();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }

  window.addEventListener("scroll", kick, { passive: true });
  window.addEventListener("resize", kick, { passive: true });
})();
