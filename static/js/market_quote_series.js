/* Piyasa kapanışları — grafik overlay script'ine bağlı kalmadan tabloya dolar. */
(function (global) {
  var cache = {};

  function pointsFromPayload(payload, seriesKey) {
    var block = payload && payload.series && payload.series[seriesKey];
    var out = [];
    ((block && block.by_date) || []).forEach(function (pt) {
      if (!pt || pt.date == null || pt.close == null || pt.close === "") return;
      var v = Number(pt.close);
      if (!Number.isFinite(v)) return;
      out.push({ key: String(pt.date).slice(0, 10), value: v });
    });
    return out;
  }

  function labelFromPayload(payload, seriesKey) {
    var block = payload && payload.series && payload.series[seriesKey];
    return (block && block.label) || seriesKey;
  }

  function load(startIso, endIso) {
    var key = (startIso || "") + "|" + (endIso || "");
    var slot = cache[key];
    if (slot && slot.payload) return Promise.resolve(slot.payload);
    if (slot && slot.pending) return slot.pending;
    var p = new URLSearchParams();
    if (startIso) p.set("start", String(startIso).slice(0, 10));
    if (endIso) p.set("end", String(endIso).slice(0, 10));
    var pending = fetch("/api/market-quotes/overlay?" + p.toString(), {
      credentials: "same-origin",
      cache: "no-store"
    }).then(function (r) {
      if (!r.ok) throw new Error("Market HTTP " + r.status);
      return r.json();
    }).then(function (data) {
      if (!data || !data.series) throw new Error("Market yanıtı boş");
      cache[key] = { payload: data };
      return data;
    }).catch(function (err) {
      if (cache[key] && cache[key].pending === pending) delete cache[key];
      throw err;
    });
    cache[key] = { pending: pending };
    return pending;
  }

  global.SeoMarketQuotes = {
    load: function (startIso, endIso, seriesKey) {
      return load(startIso, endIso).then(function (payload) {
        return {
          payload: payload,
          series: pointsFromPayload(payload, seriesKey),
          label: labelFromPayload(payload, seriesKey)
        };
      });
    }
  };
})(typeof window !== "undefined" ? window : this);
