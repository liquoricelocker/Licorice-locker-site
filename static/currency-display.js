/**
 * Shop display currency: auto-detect (ipapi.co) on first visit, manual override, localStorage.
 * All amounts are NZD base cents; checkout and server logic stay in NZD.
 */
(function () {
  var STORAGE_KEY = "currency";
  var RATES = { NZD: 1, USD: 0.58, EUR: 0.497 };
  var EU_EUR = ["FR", "DE", "IT", "ES", "NL", "BE", "IE", "PT"];

  function storageGet(key) {
    try {
      return localStorage.getItem(key);
    } catch (e) {
      return null;
    }
  }

  function storageSet(key, val) {
    try {
      localStorage.setItem(key, val);
    } catch (e) {}
  }

  function getCurrency() {
    var c = storageGet(STORAGE_KEY);
    if (c === "USD" || c === "EUR" || c === "NZD") return c;
    return "NZD";
  }

  function formatNzd(cents) {
    return "$" + (cents / 100).toFixed(2);
  }

  function formatForeign(nzdCents, code) {
    var foreignCents = Math.round(nzdCents * RATES[code]);
    var dollars = foreignCents / 100;
    if (code === "USD") return "$" + dollars.toFixed(2);
    if (code === "EUR") return "\u20AC" + dollars.toFixed(2);
    return formatNzd(nzdCents);
  }

  function formatPrice(nzdCents) {
    var code = getCurrency();
    if (code === "NZD") return formatNzd(nzdCents);
    return formatForeign(nzdCents, code);
  }

  function updatePrices() {
    var code = getCurrency();
    document.querySelectorAll("[data-nzd-cents]").forEach(function (el) {
      var cents = parseInt(el.getAttribute("data-nzd-cents"), 10);
      if (isNaN(cents)) return;
      el.textContent = formatPrice(cents);
    });
    document.querySelectorAll(".js-currency-suffix").forEach(function (el) {
      if (code === "NZD") el.textContent = "NZD";
      else if (code === "USD") el.textContent = "USD";
      else el.textContent = "EUR";
    });
  }

  function setMenuOpen(open) {
    var root = document.getElementById("currency-switcher");
    var btn = document.getElementById("currency-switcher-btn");
    var menu = document.getElementById("currency-switcher-menu");
    if (!root || !btn || !menu) return;
    document.body.classList.toggle("currency-switcher-open", open);
    btn.setAttribute("aria-expanded", open ? "true" : "false");
    menu.hidden = !open;
  }

  function updateSwitcherLabel() {
    var btn = document.getElementById("currency-switcher-btn");
    if (!btn) return;
    var label = btn.querySelector(".currency-switcher-label");
    if (!label) return;
    var c = getCurrency();
    label.textContent = c === "EUR" ? "\u20AC" : c === "USD" ? "$" : "NZ$";
    btn.setAttribute("title", "Display currency: " + c + " (checkout in NZD)");
  }

  function setCurrency(code) {
    if (!RATES[code]) return;
    storageSet(STORAGE_KEY, code);
    document.documentElement.setAttribute("data-display-currency", code);
    updatePrices();
    updateSwitcherLabel();
    setMenuOpen(false);
    document.dispatchEvent(
      new CustomEvent("licorice:currencychange", { detail: { currency: code } })
    );
  }

  function detectCurrency() {
    if (storageGet(STORAGE_KEY)) return Promise.resolve();
    return fetch("https://ipapi.co/json/", { credentials: "omit" })
      .then(function (res) {
        return res.ok ? res.json() : {};
      })
      .then(function (data) {
        if (storageGet(STORAGE_KEY)) return;
        var cc = (data && data.country_code) || "";
        if (cc === "US") setCurrency("USD");
        else if (EU_EUR.indexOf(cc) !== -1) setCurrency("EUR");
        else setCurrency("NZD");
      })
      .catch(function () {
        if (!storageGet(STORAGE_KEY)) setCurrency("NZD");
      });
  }

  function initSwitcher() {
    var root = document.getElementById("currency-switcher");
    var btn = document.getElementById("currency-switcher-btn");
    var menu = document.getElementById("currency-switcher-menu");
    if (!root || !btn || !menu) return;

    btn.addEventListener("click", function (e) {
      e.stopPropagation();
      setMenuOpen(menu.hidden);
    });

    menu.querySelectorAll("[data-currency]").forEach(function (opt) {
      opt.addEventListener("click", function () {
        var c = opt.getAttribute("data-currency");
        if (c) setCurrency(c);
      });
    });

    document.addEventListener("click", function (e) {
      if (!root.contains(e.target)) setMenuOpen(false);
    });

    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape" && document.body.classList.contains("currency-switcher-open")) {
        setMenuOpen(false);
        btn.focus();
      }
    });
  }

  document.documentElement.setAttribute("data-display-currency", getCurrency());

  document.addEventListener("DOMContentLoaded", function () {
    updatePrices();
    updateSwitcherLabel();
    initSwitcher();
    detectCurrency().then(function () {
      updatePrices();
      updateSwitcherLabel();
    });
  });

  window.licoriceCurrency = {
    setCurrency: setCurrency,
    getCurrency: getCurrency,
    refresh: updatePrices,
  };
})();
