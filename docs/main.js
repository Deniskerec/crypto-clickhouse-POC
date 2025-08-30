/* ===== Helpers ===== */
const API = document.getElementById('apiBase')?.textContent.trim();
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));
function fmt(n, d=2){ return Number(n).toLocaleString(undefined,{maximumFractionDigits:d})}
function nowIso(){ return new Date().toISOString().slice(11,19) + 'Z' }

/* ===== Tabs & Modal ===== */
const tabs = $$('.tab');
const pages = $$('.tab-page');
const modal = $('#modal');
const modalClose = $('#modalClose');
const modalTry = $('#modalTry');

let liveWarningShown = false;
function showTab(name){
  tabs.forEach(t => t.classList.toggle('active', t.dataset.tab===name));
  pages.forEach(p => p.classList.toggle('hidden', p.dataset.tab!==name));
  if(name==='live' && !liveWarningShown){
    modal.classList.remove('hidden');
  }
}
tabs.forEach(t => t.addEventListener('click', () => showTab(t.dataset.tab)));
modalClose.addEventListener('click', ()=> modal.classList.add('hidden'));
modalTry.addEventListener('click', ()=> { liveWarningShown = true; modal.classList.add('hidden'); });

/* ===== Minimal D3 renderers ===== */
function clearSvg(id){ d3.select(id).selectAll('*').remove(); }

function barCompare(id, labels, buyVals, sellVals, titleLeft='Buy', titleRight='Sell'){
  clearSvg(id);
  const svg = d3.select(id), w = svg.node().clientWidth, h = svg.node().clientHeight, m= {t:20,r:20,b:40,l:40};
  const innerW = w-m.l-m.r, innerH = h-m.t-m.b;
  const g = svg.append('g').attr('transform',`translate(${m.l},${m.t})`);

  const x0 = d3.scaleBand().domain(labels).range([0,innerW]).padding(0.2);
  const x1 = d3.scaleBand().domain(['buy','sell']).range([0,x0.bandwidth()]).padding(0.2);
  const y = d3.scaleLinear().domain([0, d3.max([...buyVals, ...sellVals])||1]).nice().range([innerH,0]);

  const xA = d3.axisBottom(x0); const yA = d3.axisLeft(y).ticks(6).tickSize(-innerW);
  g.append('g').attr('class','axis').attr('transform',`translate(0,${innerH})`).call(xA);
  g.append('g').attr('class','axis').call(yA);

  const data = labels.map((s,i)=>({sym:s,buy:buyVals[i]||0,sell:sellVals[i]||0}));
  const series = g.selectAll('.sym').data(data).enter().append('g').attr('transform',d=>`translate(${x0(d.sym)},0)`);
  series.append('rect').attr('class','buyFill').attr('x',x1('buy')).attr('y',d=>y(d.buy)).attr('width',x1.bandwidth()).attr('height',d=>innerH-y(d.buy));
  series.append('rect').attr('class','sellFill').attr('x',x1('sell')).attr('y',d=>y(d.sell)).attr('width',x1.bandwidth()).attr('height',d=>innerH-y(d.sell));
}

function lineDual(id, xs, ys1, ys2, label1='Buy', label2='Sell'){
  clearSvg(id);
  const svg = d3.select(id), w = svg.node().clientWidth, h = svg.node().clientHeight, m= {t:20,r:20,b:30,l:40};
  const innerW = w-m.l-m.r, innerH = h-m.t-m.b;
  const g = svg.append('g').attr('transform',`translate(${m.l},${m.t})`);

  const x = d3.scalePoint().domain(xs).range([0,innerW]);
  const y = d3.scaleLinear().domain([0, d3.max([...ys1,...ys2])||1]).nice().range([innerH,0]);

  const xA = d3.axisBottom(x).tickValues(xs.filter((_,i)=>i%Math.ceil(xs.length/8)===0));
  const yA = d3.axisLeft(y).ticks(6).tickSize(-innerW);
  g.append('g').attr('class','axis').attr('transform',`translate(0,${innerH})`).call(xA);
  g.append('g').attr('class','axis').call(yA);

  const line = d3.line().x((d,i)=>x(xs[i])).y(d=>y(d));
  g.append('path').attr('class','buyLine').attr('fill','none').attr('stroke','#2ecc71').attr('stroke-width',2).attr('d',line(ys1));
  g.append('path').attr('class','sellLine').attr('fill','none').attr('stroke','#e74c3c').attr('stroke-width',2).attr('d',line(ys2));
}

function barsSingle(id, xs, ys){
  clearSvg(id);
  const svg = d3.select(id), w = svg.node().clientWidth, h = svg.node().clientHeight, m= {t:20,r:20,b:30,l:40};
  const innerW = w-m.l-m.r, innerH = h-m.t-m.b;
  const g = svg.append('g').attr('transform',`translate(${m.l},${m.t})`);
  const x = d3.scaleBand().domain(xs).range([0,innerW]).padding(0.2);
  const y = d3.scaleLinear().domain([0, d3.max(ys)||1]).nice().range([innerH,0]);
  g.append('g').attr('class','axis').attr('transform',`translate(0,${innerH})`).call(d3.axisBottom(x).tickValues(xs.filter((_,i)=>i%Math.ceil(xs.length/8)===0)));
  g.append('g').attr('class','axis').call(d3.axisLeft(y).ticks(6).tickSize(-innerW));
  g.selectAll('rect').data(ys).enter().append('rect').attr('class','bar').attr('x',(d,i)=>x(xs[i])).attr('y',d=>y(d)).attr('width',x.bandwidth()).attr('height',d=>innerH-y(d));
}

/* ===== Static demo data ===== */
function rnd(n,a,b){ return Array.from({length:n},()=> a + Math.random()*(b-a)); }
const staticSyms = ['BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT','ADAUSDT'];

function buildStaticRealtime(){
  const labels = staticSyms;
  const buyVol = rnd(labels.length, 200, 1200);
  const sellVol = rnd(labels.length, 200, 1200);
  barCompare('#srt-buy-sell', labels, buyVol, sellVol);

  const avgBuy = rnd(labels.length, 80, 120);   // pretend prices
  const avgSell= avgBuy.map(v => v + (Math.random()*2-1)); // +/- 1
  lineDual('#srt-avg-prices', labels, avgBuy, avgSell);

  const tpm = rnd(labels.length, 50, 400);
  barsSingle('#srt-trades-per-min', labels, tpm);

  // fake last trades table (60s)
  const tbody = $('#srtTable tbody'); tbody.innerHTML='';
  for(let i=0;i<30;i++){
    const sym = labels[Math.floor(Math.random()*labels.length)];
    const side = Math.random()>.5?'BUY':'SELL';
    const price = (100 + Math.random()*30).toFixed(2);
    const qty = (Math.random()*0.5).toFixed(4);
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${nowIso()}</td><td>${sym}</td>
      <td class="right">${price}</td><td class="right">${qty}</td><td>${side}</td>`;
    tbody.appendChild(tr);
  }
}

function buildStaticHistorical(){
  const n= 120; // shorter series for page weight
  const xs = Array.from({length:n},(_,i)=> `${i}m`);
  const base= 100; // baseline
  const buy = Array.from({length:n},(_,i)=> base + Math.sin(i/7)*3 + Math.random()*1.2);
  const sell= buy.map((v,i)=> v + (Math.sin(i/5))*0.8 );
  lineDual('#sh-avg-prices', xs, buy, sell);

  const bv = rnd(n, 100, 500), sv = rnd(n, 100, 500);
  barCompare('#sh-buy-sell', xs.slice(-12), bv.slice(-12), sv.slice(-12)); // show last 12 buckets

  const tpm = rnd(n, 80, 250);
  barsSingle('#sh-trades-per-min', xs, tpm);

  $('#shRows').textContent = fmt(n);
  $('#shAvgTrades').textContent = fmt(d3.mean(tpm));
}

/* ===== Live (API) wiring (unchanged visuals) ===== */
async function getJSON(path){
  if(!API) throw new Error('No API base URL');
  const res = await fetch(`${API}${path}`);
  if(!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

async function refreshLive(){
  // top symbols buy/sell + avg prices + trades/min
  const minutes = Number($('#liveMinutes').value||10);
  $('#liveMinLabel').textContent = minutes;

  const top = await getJSON(`/top_symbols?minutes=${minutes}&limit=${Number($('#liveTop').value||5)}`);
  const labels = top.map(d=>d.symbol);
  const buyVol = top.map(d=>d.buy_volume);
  const sellVol= top.map(d=>d.sell_volume);
  barCompare('#live-buy-sell', labels, buyVol, sellVol);

  const avgBuy = top.map(d=>d.avg_buy_price);
  const avgSell= top.map(d=>d.avg_sell_price);
  lineDual('#live-avg-prices', labels, avgBuy, avgSell);

  const tpm = top.map(d=>d.trades_per_min);
  barsSingle('#live-trades-per-min', labels, tpm);

  // live table
  const sym = $('#liveSymbol').value || 'BTCUSDT';
  $('#liveSymbolLabel').textContent = sym;
  $('#liveWindowLabel').textContent = Number($('#liveWindow').value||60);
  const rows = await getJSON(`/live_trades?window_sec=${Number($('#liveWindow').value||60)}&symbol=${sym}`);
  const tb = $('#liveTable tbody'); tb.innerHTML='';
  rows.forEach(r=>{
    const side = r.is_buyer_maker? 'SELL':'BUY';
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${r.ts}</td><td>${r.symbol}</td>
      <td class="right">${fmt(r.price,2)}</td><td class="right">${fmt(r.qty,4)}</td><td>${side}</td>`;
    tb.appendChild(tr);
  });
  $('#statusLive').textContent = `Rows: ${rows.length}`;
}

async function refreshHist(){
  const m = Number($('#histMinutes').value||360);
  const sym = $('#histSymbol').value || 'BTCUSDT';
  $('#histMinLabel').textContent = m; $('#histSymbolLabel').textContent = sym;

  const rows = await getJSON(`/ohlcv?symbol=${encodeURIComponent(sym)}&minutes=${m}`);
  const xs = rows.map(r=>r.minute.slice(11,16));
  const buy = rows.map(r=>r.avg_buy_price);
  const sell= rows.map(r=>r.avg_sell_price);
  lineDual('#hist-avg-prices', xs, buy, sell);

  const bv = rows.map(r=>r.buy_volume);
  const sv = rows.map(r=>r.sell_volume);
  barCompare('#hist-buy-sell', xs.slice(-12), bv.slice(-12), sv.slice(-12));

  const tpm = rows.map(r=>r.trades);
  barsSingle('#hist-trades-per-min', xs, tpm);
  $('#histRows').textContent = fmt(rows.length);
  $('#histAvgTrades').textContent = fmt(d3.mean(tpm));
}

/* ===== Buttons ===== */
$('#applyLive').addEventListener('click', ()=> refreshLive().catch(()=>{}));
$('#applyHist').addEventListener('click', ()=> refreshHist().catch(()=>{}));

$('#btnStart').addEventListener('click', async ()=>{
  if(!API){ alert('No API available on static site.'); return; }
  try{
    await fetch(`${API}/collector/start`,{method:'POST'});
  }catch{}
});
$('#btnStop').addEventListener('click', async ()=>{
  if(!API){ return; }
  try{ await fetch(`${API}/collector/stop`,{method:'POST'});}catch{}
});

/* ===== First render ===== */
showTab('static-rt');      // default to the static demo
buildStaticRealtime();
buildStaticHistorical();   // prepares static historical too

// If user navigates between tabs, (re)draw static as needed
document.querySelector('[data-tab="static-rt"]').addEventListener('click', buildStaticRealtime);
document.querySelector('[data-tab="static-hist"]').addEventListener('click', buildStaticHistorical);

// Optional: try to ping status to show if backend exists
(async ()=>{
  if(!API) return;
  try{
    const r = await fetch(`${API}/collector/status`);
    if(r.ok){
      const s = await r.json();
      $('#collectorStatus').textContent = `collector: ${s.status||'unknown'}`;
    }
  }catch{
    $('#collectorStatus').textContent = 'collector: offline (static demo)';
  }
})();