// ---------- Config & Tabs ----------
const apiBase = document.getElementById("apiBase")?.textContent?.trim() || "http://127.0.0.1:8000";
const tabs = document.querySelectorAll(".tab");
const pages = document.querySelectorAll(".tab-page");
tabs.forEach(t => t.addEventListener("click", () => {
  tabs.forEach(x => x.classList.remove("active"));
  t.classList.add("active");
  const tab = t.dataset.tab;
  pages.forEach(p => p.classList.toggle("hidden", p.dataset.tab !== tab));
}));

// Info popups
document.querySelectorAll(".info").forEach(b => {
  b.addEventListener("click", () => {
    const topic = b.dataset.info;
    const text = topic === "live"
      ? `Live (last N minutes):
- Buy vs Sell uses Binance 'm' flag
  is_buyer_maker=0 → BUY, 1 → SELL
- Avg prices are VWAP per side: sum(price*qty)/sum(qty)
- Live table shows newest 500 trades from rolling window`
      : `Historical:
- Per-minute series of BUY/SELL volume and VWAP prices
- Trades per minute shows activity density`;
    alert(text);
  });
});

// ---------- Collector controls ----------
const collectorStatus = document.getElementById("collectorStatus");
document.getElementById("btnStart").onclick = async () => {
  try {
    const r = await fetch(`${apiBase}/collector/start`, { method: "POST" });
    const j = await r.json();
    collectorStatus.textContent = j.status?.running ? "Running" : "Starting…";
  } catch (e) { collectorStatus.textContent = "Start error"; }
};
document.getElementById("btnStop").onclick = async () => {
  try {
    await fetch(`${apiBase}/collector/stop`, { method: "POST" });
    collectorStatus.textContent = "Stopped";
  } catch (e) { collectorStatus.textContent = "Stop error"; }
};
setInterval(async () => {
  try {
    const s = await (await fetch(`${apiBase}/collector/status`)).json();
    collectorStatus.textContent = s.running
      ? `Running • rows: ${s.inserted_rows} • last flush: ${s.last_flush ?? "n/a"}`
      : "Stopped";
    if (s.last_error) collectorStatus.textContent = `Error: ${s.last_error}`;
  } catch {}
}, 3000);

// ---------- Helpers ----------
async function fetchJSON(url) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`${url} → HTTP ${r.status}`);
  return r.json();
}
function fmt(n, d = 2) { return d3.format(`,.${d}f`)(+n || 0); }
function fmt0(n){ return d3.format(",d")(+n || 0); }

// ---------- Filters (sidebar) ----------
const liveWindowInp  = document.getElementById("liveWindow");
const liveMinutesInp = document.getElementById("liveMinutes");
const liveTopInp     = document.getElementById("liveTop");
const liveSymbolSel  = document.getElementById("liveSymbol");

const histSymbolSel  = document.getElementById("histSymbol");
const histMinutesInp = document.getElementById("histMinutes");

document.getElementById("applyLive").onclick = () => {
  document.getElementById("liveMinLabel").textContent = liveMinutesInp.value;
  document.getElementById("liveWindowLabel").textContent = liveWindowInp.value;
  document.getElementById("liveSymbolLabel").textContent = liveSymbolSel.value;
  loadLive(); loadLiveTable();
};

document.getElementById("applyHist").onclick = () => {
  document.getElementById("histSymbolLabel").textContent = histSymbolSel.value;
  document.getElementById("histMinLabel").textContent = histMinutesInp.value;
  loadHist();
};

// ---------- Live charts ----------
async function loadLive() {
  const mins = +liveMinutesInp.value || 10;
  const top  = +liveTopInp.value || 5;

  const liveBS = await fetchJSON(`${apiBase}/live_buy_sell?minutes=${mins}&top=${top}`);

  drawGroupedBars("#live-buy-sell", liveBS, ["buy_volume","sell_volume"], ["Buy","Sell"], ["buyFill","sellFill"]);
  drawGroupedBars("#live-avg-prices", liveBS, ["avg_buy_price","avg_sell_price"], ["Avg Buy","Avg Sell"], ["bar","warnFill"]);
  drawBars("#live-trades-per-min", liveBS.map(d => ({ label: d.symbol, value: +d.trades_per_min })));
}
setInterval(loadLive, 5000);
loadLive();

// ---------- Live table (rolling window) ----------
const liveTBody = document.querySelector("#liveTable tbody");
async function loadLiveTable() {
  const sym = liveSymbolSel.value;
  const win = +liveWindowInp.value || 60;
  try {
    const rows = await fetchJSON(`${apiBase}/live_trades?symbol=${sym}&window_sec=${win}`);
    const sLive = document.getElementById("statusLive");
    sLive.textContent = `${rows.length} trades`;

    liveTBody.innerHTML = rows.map(r => `
      <tr>
        <td>${r.ts}</td>
        <td>${r.symbol}</td>
        <td class="right">${fmt(r.price)}</td>
        <td class="right">${fmt(r.qty, 6)}</td>
        <td><span class="tag">${r.is_buyer_maker ? "Sell" : "Buy"}</span></td>
      </tr>
    `).join("");
  } catch (e) {
    document.getElementById("statusLive").textContent = "Error loading trades";
  }
}
setInterval(loadLiveTable, 3000);
loadLiveTable();

// ---------- Historical ----------
async function loadHist() {
  const sym = histSymbolSel.value;
  const mins = +histMinutesInp.value || 360;

  const data = await fetchJSON(`${apiBase}/hist_buy_sell?symbol=${sym}&minutes=${mins}`);
  document.getElementById("histRows").textContent = fmt0(data.length);
  const avgTrades = data.length ? d3.mean(data, d => +d.trades || 0) : 0;
  document.getElementById("histAvgTrades").textContent = fmt(avgTrades, 1);

  drawTwoLines(
    "#hist-buy-sell",
    data.map(d => ({ t: new Date(d.minute), y: +d.buy_volume })),
    data.map(d => ({ t: new Date(d.minute), y: +d.sell_volume })),
    "Buy Vol", "Sell Vol"
  );

  drawTwoLines(
    "#hist-avg-prices",
    data.map(d => ({ t: new Date(d.minute), y: +d.avg_buy_price })),
    data.map(d => ({ t: new Date(d.minute), y: +d.avg_sell_price })),
    "Avg Buy", "Avg Sell"
  );

  drawBars(
    "#hist-trades-per-min",
    data.map(d => ({ label: d.minute.slice(11,16), value: +d.trades }))
  );
}
loadHist();

// ---------- Chart helpers ----------
function drawGroupedBars(selector, rows, fields, labels, classes){
  const svg = d3.select(selector);
  const W = svg.node().clientWidth || 960, H = 420, M = {t:20,r:20,b:40,l:70};
  svg.attr("width", W).attr("height", H);
  svg.selectAll("*").remove();
  const g = svg.append("g").attr("transform", `translate(${M.l},${M.t})`);
  const iw = W - M.l - M.r, ih = H - M.t - M.b;

  const x0 = d3.scaleBand().domain(rows.map(d => d.symbol)).range([0, iw]).padding(0.25);
  const x1 = d3.scaleBand().domain(fields).range([0, x0.bandwidth()]).padding(0.15);
  const maxY = d3.max(rows, d => d3.max(fields, f => +d[f] || 0)) || 1;
  const y  = d3.scaleLinear().domain([0, maxY]).nice().range([ih, 0]);

  g.append("g").attr("class","axis x").attr("transform",`translate(0,${ih})`).call(d3.axisBottom(x0));
  g.append("g").attr("class","axis y").call(d3.axisLeft(y));

  const cat = g.selectAll(".cat").data(rows).enter().append("g").attr("class","cat")
    .attr("transform", d => `translate(${x0(d.symbol)},0)`);

  fields.forEach((f, i) => {
    cat.append("rect")
      .attr("class", classes[i] || "bar")
      .attr("x", x1(f))
      .attr("width", x1.bandwidth())
      .attr("y", d => y(+d[f] || 0))
      .attr("height", d => ih - y(+d[f] || 0))
      .append("title").text(d => `${labels[i]}: ${fmt(d[f])}`);
  });
}

function drawBars(selector, rows){
  const svg = d3.select(selector);
  const W = svg.node().clientWidth || 960, H = 420, M = {t:20,r:20,b:40,l:70};
  svg.attr("width", W).attr("height", H);
  svg.selectAll("*").remove();
  const g = svg.append("g").attr("transform", `translate(${M.l},${M.t})`);
  const iw = W - M.l - M.r, ih = H - M.t - M.b;

  const x = d3.scaleBand().domain(rows.map(r => r.label)).range([0, iw]).padding(0.25);
  const y = d3.scaleLinear().domain([0, d3.max(rows, r => +r.value)||1]).nice().range([ih, 0]);

  g.append("g").attr("transform",`translate(0,${ih})`).call(d3.axisBottom(x));
  g.append("g").call(d3.axisLeft(y));

  g.selectAll("rect").data(rows).enter().append("rect")
    .attr("class","bar")
    .attr("x", d => x(d.label))
    .attr("y", d => y(+d.value))
    .attr("width", x.bandwidth())
    .attr("height", d => ih - y(+d.value))
    .append("title").text(d => `${d.label}: ${fmt(d.value)}`);
}

function drawTwoLines(selector, a, b, nameA, nameB){
  const svg = d3.select(selector);
  const W = svg.node().clientWidth || 960, H = 420, M = {t:20,r:20,b:40,l:60};
  svg.attr("width", W).attr("height", H);
  svg.selectAll("*").remove();
  const g = svg.append("g").attr("transform",`translate(${M.l},${M.t})`);
  const iw = W - M.l - M.r, ih = H - M.t - M.b;

  const all = a.concat(b);
  if (!all.length){ return; }
  const x = d3.scaleTime()
    .domain(d3.extent(all, d => d.t))
    .range([0, iw]);
  const y = d3.scaleLinear()
    .domain([d3.min(all, d => d.y)||0, d3.max(all, d => d.y)||1]).nice()
    .range([ih, 0]);

  g.append("g").attr("transform",`translate(0,${ih})`).call(d3.axisBottom(x));
  g.append("g").call(d3.axisLeft(y));

  const line = d3.line().x(d => x(d.t)).y(d => y(d.y));
  g.append("path").datum(a).attr("fill","none").attr("stroke","var(--buy)").attr("stroke-width",1.5).attr("d", line);
  g.append("path").datum(b).attr("fill","none").attr("stroke","var(--sell)").attr("stroke-width",1.5).attr("d", line);

  g.append("text").attr("x", 8).attr("y", 12).text(nameA).attr("fill","var(--buy)");
  g.append("text").attr("x", 8).attr("y", 28).text(nameB).attr("fill","var(--sell)");
}