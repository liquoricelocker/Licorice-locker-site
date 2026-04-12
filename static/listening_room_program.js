/**
 * Earnings calculator: per-sale tier rates (monthly order index) + milestone bonuses.
 * Prices from data attributes on #lr-calc-root (cents, NZD).
 */
(function () {
  var root = document.getElementById("lr-calc-root");
  if (!root) return;

  var SW = parseInt(root.getAttribute("data-soundwave-cents") || "42900", 10);
  var MINI = parseInt(root.getAttribute("data-mini-cents") || "14300", 10);

  var slider = document.getElementById("lr-calc-slider");
  var input = document.getElementById("lr-calc-input");
  var outTotal = document.getElementById("lr-calc-total");
  var outDetail = document.getElementById("lr-calc-detail");
  var outSpins = document.getElementById("lr-calc-spins-label");
  var radios = root.querySelectorAll('input[name="lr-product-mode"]');

  function rateForNth(n) {
    if (n >= 25) return 0.3;
    if (n >= 10) return 0.25;
    return 0.2;
  }

  function milestoneBonusCents(n) {
    var b = 0;
    if (n >= 10) b += 10000;
    if (n >= 25) b += 30000;
    return b;
  }

  function priceForSaleIndex(mode, i) {
    if (mode === "mini") return MINI;
    if (mode === "mix") return i % 2 === 1 ? SW : MINI;
    return SW;
  }

  function commissionCents(n, mode) {
    var comm = 0;
    for (var s = 1; s <= n; s++) {
      var p = priceForSaleIndex(mode, s);
      comm += Math.round(p * rateForNth(s));
    }
    return comm + milestoneBonusCents(n);
  }

  function formatMoney(cents) {
    return "$" + (cents / 100).toFixed(2);
  }

  function syncFromSlider() {
    var v = parseInt(slider.value, 10) || 0;
    if (input) input.value = v;
    update(v);
  }

  function syncFromInput() {
    var v = parseInt(input.value, 10);
    if (isNaN(v) || v < 0) v = 0;
    if (v > 100) v = 100;
    if (slider) slider.value = v;
    update(v);
  }

  function currentMode() {
    var r = root.querySelector('input[name="lr-product-mode"]:checked');
    return r ? r.value : "soundwave";
  }

  function updateModeStyles() {
    radios.forEach(function (r) {
      var lab = r.closest(".lr-calc-mode");
      if (lab) lab.classList.toggle("is-selected", r.checked);
    });
  }

  function update(n) {
    updateModeStyles();
    var mode = currentMode();
    var total = commissionCents(n, mode);
    var bonus = milestoneBonusCents(n);
    var subtotal = total - bonus;

    if (outTotal) {
      outTotal.textContent = formatMoney(total);
      outTotal.classList.remove("lr-calc-pop");
      void outTotal.offsetWidth;
      outTotal.classList.add("lr-calc-pop");
    }
    if (outSpins) {
      outSpins.textContent =
        n === 1 ? "1 spin this month" : n + " spins this month";
    }
    if (outDetail) {
      var lines = [];
      lines.push("Estimated share from sales: " + formatMoney(subtotal));
      if (bonus > 0) {
        lines.push(
          "Including room bonuses at 10 & 25 spins: +" + formatMoney(bonus)
        );
      } else if (n >= 1 && n < 10) {
        lines.push("Hit 10 spins in a month for an extra $100 room bonus.");
      }
      if (n >= 10 && n < 25) {
        lines.push("25 spins unlocks 30% on those sales + a $300 room bonus.");
      }
      outDetail.innerHTML = lines.map(function (t) {
        return "<p>" + t + "</p>";
      }).join("");
    }
  }

  if (slider) {
    slider.addEventListener("input", syncFromSlider);
    slider.addEventListener("change", syncFromSlider);
  }
  if (input) {
    input.addEventListener("change", syncFromInput);
    input.addEventListener("input", syncFromInput);
  }
  radios.forEach(function (r) {
    r.addEventListener("change", function () {
      syncFromSlider();
    });
  });

  updateModeStyles();
  syncFromSlider();
})();
