/**
 * Global full-page loader (body.loading). showLoader / hideLoader with ~150ms show delay to avoid flicker.
 */
(function () {
  var SHOW_DELAY_MS = 150;
  var showTimer = null;

  function setAriaBusy(on) {
    var el = document.getElementById("global-loader");
    if (!el) return;
    el.setAttribute("aria-busy", on ? "true" : "false");
  }

  function showLoader() {
    if (document.body.classList.contains("loading")) return;
    if (showTimer !== null) return;
    showTimer = window.setTimeout(function () {
      showTimer = null;
      document.body.classList.add("loading");
      setAriaBusy(true);
    }, SHOW_DELAY_MS);
  }

  function hideLoader() {
    if (showTimer !== null) {
      window.clearTimeout(showTimer);
      showTimer = null;
    }
    document.body.classList.remove("loading");
    setAriaBusy(false);
  }

  window.showLoader = showLoader;
  window.hideLoader = hideLoader;

  function onPageReady() {
    hideLoader();
  }

  if (document.readyState === "complete") {
    onPageReady();
  } else {
    window.addEventListener("load", onPageReady);
  }
  window.addEventListener("pageshow", onPageReady);

  document.addEventListener(
    "click",
    function (e) {
      if (e.defaultPrevented) return;
      if (e.button !== 0) return;
      if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
      var t = e.target;
      if (!t || !t.closest) return;
      var a = t.closest("a[href]");
      if (!a) return;
      if (a.target === "_blank" || a.hasAttribute("download")) return;
      var href = a.getAttribute("href");
      if (!href || href.charAt(0) === "#" || href.indexOf("javascript:") === 0) return;
      if (href.indexOf("mailto:") === 0 || href.indexOf("tel:") === 0) return;
      try {
        var u = new URL(a.href, window.location.href);
        if (u.origin !== window.location.origin) return;
      } catch (err) {
        return;
      }
      showLoader();
    },
    false
  );

  document.addEventListener(
    "submit",
    function (e) {
      if (e.defaultPrevented) return;
      var form = e.target;
      if (!form || form.nodeName !== "FORM") return;
      if (form.hasAttribute("data-no-global-loader")) return;
      showLoader();
    },
    false
  );
})();
