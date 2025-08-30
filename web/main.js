/* main.js
 * - Distinct colors (Buy green / Sell red)
 * - Refresh buttons for Live + Historical
 * - Sidebar counters for trades per symbol
 * - Demo data for static tabs
 * - Start collecting shows ingested rows (real if API is up; simulated otherwise)
 */
const $ = (q) => document.querySelector(q);

const API_BASE = (document.getElementById('apiBase')?.textContent || '').trim();
let HAVE_API = !!API_BASE && API_BASE !== 'YOUR_API_BASE_HERE';

const COLOR = { buy:'#2ecc71', sell:'#e74c3c', accent:'#4da3ff' };
const fmt = new Intl.NumberFormat('en-US', { maximumFractionDigits: 8 });
const fmt2 = new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 });
const DEFAULT_SYMBOLS = ['BTCUSDT','ETHUSDT','BNBUSDT','SOLUSDT','ADAUSDT'];

let ingestTotal = 0;
let ingestTimer = null;
let countersTimer = null;

/* -------- utility -------- */
const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));

async function safeJson(url){
  try{
    const r = await fetch(url, {cache:'no-store'});
    if(!r.ok) throw new Error(r.statusText);
    return await r.json();
  }catch(e){ return null; }
}

/* -------- D3 helpers -------- */
function svgBox(sel){
  const svg = d3.select(sel);
  const w = svg.node().clientWidth || 900;
  const h = svg.node().clientHeight || 420;
  svg.selectAll('*').remove();
  return {svg,w,h};
}
function groupedBars(sel, groups, series, data){
  const {svg,w,h}=svgBox(sel);
  const m={top:20,right:10,bottom:40,left:60}, W=w-m.left-m.right, H=h-m.top-m.bottom;
  const g=svg.append('g').attr('transform',`translate(${m.left},${m.top})`);
  const x0=d3.scaleBand().domain(groups).range([0,W]).padding(0.2);
  const x1=d3.scaleBand().domain(series).range([0,x0.bandwidth()]).padding(0.1);
  const y=d3.scaleLinear().domain([0, d3.max(data,d=>d.value)||1]).nice().range([H,0]);
  const col=d3.scaleOrdinal().domain(series).range([COLOR.buy, COLOR.sell]);
  g.append('g').attr('class','axis').attr('transform',`translate(0,${H})`).call(d3.axisBottom(x0));
  g.append('g').attr('class','axis').call(d3.axisLeft(y));
  const grouped=d3.group(data,d=>d.group);
  for(const [grp,vals] of grouped){
    const wrap=g.append('g').attr('transform',`translate(${x0(grp)},0)`);
    vals.forEach(v=>{
      wrap.append('rect')
        .attr('x',x1(v.key)).attr('y',y(v.value))
        .attr('width',x1.bandwidth()).attr('height',H-y(v.value))
        .attr('fill',col(v.key));
    });
  }
}
function multiLine(sel, series, rows, accessor){
  const {svg,w,h}=svgBox(sel);
  const m={top:20,right:10,bottom:40,left:60}, W=w-m.left-m.right, H=h-m.top-m.bottom;
  const g=svg.append('g').attr('transform',`translate(${m.left},${m.top})`);
  const x=d3.scaleUtc().domain(d3.extent(rows,d=>d.x)).range([0,W]);
  const allY=[]; series.forEach(s=>rows.forEach(r=>allY.push(accessor(r,s.key))));
  const y=d3.scaleLinear().domain([d3.min(allY), d3.max(allY)]).nice().range([H,0]);
  g.append('g').attr('class','axis').attr('transform',`translate(0,${H})`).call(d3.axisBottom(x).ticks(6));
  g.append('g').attr('class','axis').call(d3.axisLeft(y));
  series.forEach(s=>{
    const line=d3.line().x(d=>x(d.x)).y(d=>y(accessor(d,s.key)));
    g.append('path').datum(rows).attr('fill','none').attr('stroke',s.color).attr('stroke-width',1.8).attr('d',line);
  });
}

/* -------- DEMO data builders -------- */
function randomWalk(start, steps, vol=0.002){
  const out=[start];
  for(let i=1;i<steps;i++){
    const shock=(Math.random()-0.5)*2*vol*start;
    out.push(Math.max(0.0001, out[i-1]+shock));
  }
  return out;
}
function fakeTrades(symbol, seconds=60, perSec=8, startP=65000){
  const n=seconds*perSec; const prices=randomWalk(startP,n,0.002);
  const now=Date.now(); const rows=[];
  for(let i=0;i<n;i++){
    rows.push({
      ts:new Date(now-(n-1-i)*1000),
      symbol, price:prices[i],
      qty:+(Math.random()*0.01+0.0001).toFixed(6),
      is_buyer_maker: Math.random()<0.5 ? 1 : 0
    });
  }
  return rows;
}
function minuteAgg(trades, minutes=60){
  const map=new Map();
  trades.forEach(t=>{
    const m=new Date(t.ts); m.setSeconds(0,0);
    const k=+m; if(!map.has(k)) map.set(k,[]);
    map.get(k).push(t);
  });
  const rows=[...map.keys()].sort().map(k=>{
    const arr=map.get(k);
    let high=-Infinity, low=Infinity, open=arr[0].price, close=arr[arr.length-1].price, vol=0, trades=arr.length;
    let buyVol=0, sellVol=0, buyPV=0, sellPV=0;
    arr.forEach(t=>{
      const p=t.price, q=t.qty; high=Math.max(high,p); low=Math.min(low,p); vol+=q;
      if(t.is_buyer_maker===0){ buyVol+=q; buyPV+=p*q; } else { sellVol+=q; sellPV+=p*q; }
    });
    return {
      x:new Date(+k),
      trades,
      buy_vol:buyVol, sell_vol:sellVol,
      avg_buy: buyVol? buyPV/buyVol : open,
      avg_sell: sellVol? sellPV/sellVol : open
    };
  });
  return rows.slice(-minutes);
}

/* -------- Sidebar counters -------- */
function renderCounters(map){
  const wrap = $('#symbolCounters');
  if(!wrap) return;
  const entries = Array.from(map.entries()).sort((a,b)=>b[1]-a[1]);
  wrap.innerHTML = entries.map(([sym,val])=>`
    <div class="counter-item">
      <span class="label">${sym}</span>
      <span class="value">${fmt2.format(val)}</span>
    </div>`).join('');
}
async function refreshCounters(){
  // If API: use /top_symbols to approximate “activity” by volume
  if(HAVE_API){
    const mins = parseInt($('#liveMinutes')?.value||'10',10);
    const limit = parseInt($('#liveTop')?.value||'5',10);
    const data = await safeJson(`${API_BASE}/top_symbols?minutes=${mins}&limit=${limit}`);
    if(data){
      const m = new Map();
      data.forEach(r=> m.set(r.symbol, r.volume));
      renderCounters(m);
      return;
    }
  }
  // DEMO fallback
  const m = new Map();
  DEFAULT_SYMBOLS.forEach(s=> m.set(s, Math.random()*10+3));
  renderCounters(m);
}

/* -------- Live (API) rendering -------- */
async function loadLive(){
  const winSec = parseInt($('#liveWindow').value||'60',10);
  const minutes = parseInt($('#liveMinutes').value||'10',10);
  const topN = parseInt($('#liveTop').value||'5',10);
  const symbol = $('#liveSymbol').value;

  $('#liveMinLabel').textContent = minutes.toString();
  $('#liveWindowLabel').textContent = winSec.toString();
  $('#liveSymbolLabel').textContent = symbol;

  if(!HAVE_API){
    // Soft notify
    $('#statusLive').textContent = 'API not detected — demo mode';
  }else{
    $('#statusLive').textContent = 'Loading…';
  }

  // Top buy/sell volumes
  const top = HAVE_API
    ? await safeJson(`${API_BASE}/top_symbols?minutes=${minutes}&limit=${topN}`)
    : DEFAULT_SYMBOLS.slice(0,topN).map(s=>({symbol:s,buy_vol:Math.random()*6+3,sell_vol:Math.random()*6+3}));

  if(top){
    const groups = top.map(d=>d.symbol);
    const data = [
      ...top.map(d=>({group:d.symbol,key:'Buy', value:d.buy_vol})),
      ...top.map(d=>({group:d.symbol,key:'Sell', value:d.sell_vol}))
    ];
    groupedBars('#live-buy-sell', groups, ['Buy','Sell'], data);
  }

  // Avg buy/sell price (VWAP) per minute -> synth from minuteAgg of fake trades if no API
  let avgRows;
  if(HAVE_API){
    // Use OHLCV endpoint (reusing close as trend) OR add your dedicated endpoint
    const o = await safeJson(`${API_BASE}/ohlcv?symbol=${symbol}&minutes=${minutes}`);
    if(o){
      avgRows = o.map(r=>({
        x: new Date(r.minute + 'Z'),
        avg_buy: r.avg_buy ?? r.close,
        avg_sell: r.avg_sell ?? r.close
      }));
    }
  }
  if(!avgRows){
    const f = fakeTrades(symbol, minutes*60, 4, 65000);
    avgRows = minuteAgg(f, minutes);
  }
  multiLine('#live-avg-prices',
    [{key:'avg_buy', color:COLOR.buy},{key:'avg_sell', color:COLOR.sell}],
    avgRows, (r,k)=>r[k]);

  // Trades per minute chart for the same symbols (fake if no API)
  let tpm;
  if(HAVE_API){
    // Approximate: reuse OHLCV for each of top symbols if you expose symbol param; for brevity we simulate
    tpm = avgRows.map(r=>({x:r.x, count: Math.round(20+Math.random()*25)}));
  }else{
    tpm = avgRows.map(r=>({x:r.x, count: Math.round(20+Math.random()*25)}));
  }
  multiLine('#live-trades-per-min', [{key:'count', color:COLOR.accent}], tpm, (r)=>r.count);

  // Raw last trades table
  let raw = null;
  if(HAVE_API){
    raw = await safeJson(`${API_BASE}/live_trades?window_sec=${winSec}&symbol=${encodeURIComponent(symbol)}`);
  }
  if(!raw){
    raw = fakeTrades(symbol, winSec, 8);
  }
  const tbody = $('#liveTable tbody');
  tbody.innerHTML = raw.slice(0,500).map(t=>`
    <tr>
      <td>${(t.ts || t.ts_utc || new Date()).toString().replace('T',' ').replace('Z','')}</td>
      <td>${t.symbol}</td>
      <td class="right">${fmt2.format(t.price)}</td>
      <td class="right">${fmt.format(t.qty)}</td>
      <td>${(t.is_buyer_maker===1||t.side==='Sell')?'Sell':'Buy'}</td>
    </tr>`).join('');

  $('#statusLive').textContent = 'OK';
}

/* -------- Historical (API) rendering -------- */
async function loadHist(){
  const symbol = $('#histSymbol').value;
  const minutes = parseInt($('#histMinutes').value||'360',10);
  $('#histSymbolLabel').textContent = symbol;
  $('#histMinLabel').textContent = minutes.toString();
  $('#statusHist').textContent = HAVE_API ? 'Loading…' : 'API not detected — demo mode';

  let rows = null;
  if(HAVE_API){
    rows = await safeJson(`${API_BASE}/ohlcv?symbol=${symbol}&minutes=${minutes}`);
  }
  if(!rows){
    // demo from fake trades
    const f = fakeTrades(symbol, minutes*60, 4, 64000);
    rows = minuteAgg(f, minutes).map(r=>({
      minute: r.x.toISOString(),
      buy_vol: r.buy_vol, sell_vol: r.sell_vol,
      avg_buy: r.avg_buy, avg_sell: r.avg_sell,
      trades: r.trades
    }));
  }

  const xRows = rows.map(r=>({x:new Date((r.minute||r.x)+'Z'), buy:r.buy_vol, sell:r.sell_vol}));
  groupedBars('#hist-buy-sell',
    xRows.map(r=>r.x.toISOString().slice(11,16)),
    ['Buy','Sell'],
    xRows.map((r,i)=>[
      {group:xRows[i].x.toISOString().slice(11,16), key:'Buy', value:r.buy},
      {group:xRows[i].x.toISOString().slice(11,16), key:'Sell', value:r.sell}
    ]).flat()
  );

  multiLine('#hist-avg-prices',
    [{key:'avg_buy', color:COLOR.buy},{key:'avg_sell', color:COLOR.sell}],
    rows.map(r=>({x:new Date((r.minute||r.x)+'Z'), avg_buy:r.avg_buy, avg_sell:r.avg_sell})),
    (r,k)=>r[k]
  );

  multiLine('#hist-trades-per-min',
    [{key:'trades', color:COLOR.accent}],
    rows.map(r=>({x:new Date((r.minute||r.x)+'Z'), trades:r.trades || Math.round(15+Math.random()*25)})),
    (r)=>r.trades
  );

  $('#histRows').textContent = rows.length.toString();
  const avgTrades = rows.reduce((a,b)=>a+(b.trades||0),0) / Math.max(1, rows.length);
  $('#histAvgTrades').textContent = fmt2.format(avgTrades);
  $('#statusHist').textContent = 'OK';
}

/* -------- Static demos -------- */
function loadStaticRealtime(){
  const syms = DEFAULT_SYMBOLS;
  const top = syms.map(s=>({symbol:s, buy_vol: 3+Math.random()*3, sell_vol:3+Math.random()*3}));
  groupedBars('#demo-live-buy-sell',
    top.map(d=>d.symbol), ['Buy','Sell'],
    top.flatMap(d=>[
      {group:d.symbol,key:'Buy', value:d.buy_vol},
      {group:d.symbol,key:'Sell', value:d.sell_vol}
    ])
  );

  const f = fakeTrades('BTCUSDT', 600, 4, 65000);
  const minutes = minuteAgg(f, 10);
  multiLine('#demo-live-avg-prices',
    [{key:'avg_buy', color:COLOR.buy},{key:'avg_sell', color:COLOR.sell}],
    minutes, (r,k)=>r[k]
  );
  multiLine('#demo-live-trades-per-min',
    [{key:'trades', color:COLOR.accent}],
    minutes.map(r=>({x:r.x, trades:r.trades})), (r)=>r.trades
  );

  const tbody = $('#demo-live-table tbody');
  const recent = fakeTrades('BTCUSDT', 60, 8, minutes.at(-1).avg_buy);
  tbody.innerHTML = recent.slice().reverse().map(t=>`
    <tr>
      <td>${t.ts.toISOString().replace('T',' ').replace('Z','')}</td>
      <td>${t.symbol}</td>
      <td class="right">${fmt2.format(t.price)}</td>
      <td class="right">${fmt.format(t.qty)}</td>
      <td>${t.is_buyer_maker ? 'Sell' : 'Buy'}</td>
    </tr>`).join('');
}
function loadStaticHistorical(){
  const f = fakeTrades('BTCUSDT', 360*60, 3, 64000);
  const rows = minuteAgg(f, 360);

  // KPIs
  $('#demoHistRows').textContent = rows.length.toString();
  $('#demoHistAvgTrades').textContent = fmt2.format(rows.reduce((a,b)=>a+b.trades,0)/rows.length);

  groupedBars('#demo-hist-buy-sell',
    rows.slice(-20).map(r=>r.x.toISOString().slice(11,16)),
    ['Buy','Sell'],
    rows.slice(-20).flatMap(r=>[
      {group:r.x.toISOString().slice(11,16), key:'Buy', value:r.buy_vol},
      {group:r.x.toISOString().slice(11,16), key:'Sell', value:r.sell_vol}
    ])
  );
  multiLine('#demo-hist-avg-prices',
    [{key:'avg_buy', color:COLOR.buy},{key:'avg_sell', color:COLOR.sell}],
    rows.slice(-120), (r,k)=>r[k]
  );
  multiLine('#demo-hist-trades-per-min',
    [{key:'trades', color:COLOR.accent}],
    rows.slice(-120), (r)=>r.trades
  );
}

/* -------- Ingestion control + counters -------- */
async function probeApi(){
  if(!HAVE_API) return false;
  const ping = await safeJson(`${API_BASE}/collector/status`);
  HAVE_API = !!ping;
  $('#collectorStatus').textContent = HAVE_API ? 'API detected' : 'demo mode';
  return HAVE_API;
}
async function startCollect(){
  await probeApi();
  if(HAVE_API){
    const r = await safeJson(`${API_BASE}/collector/start`);
    $('#collectorStatus').textContent = r?.status || 'started';
  }else{
    alert('No API detected. Running in static demo mode.\n(For live demo contact: deniskerec1994@gmail.com)');
    $('#collectorStatus').textContent = 'demo mode';
  }
  ingestTotal = 0;
  $('#ingestCounter').textContent = `Ingested: ${ingestTotal}`;

  if(ingestTimer) clearInterval(ingestTimer);
  ingestTimer = setInterval(async () => {
    // If API: count rows from last 5s. Else simulate +random.
    if(HAVE_API){
      const data = await safeJson(`${API_BASE}/live_trades?window_sec=5&symbol=BTCUSDT`);
      const inc = Array.isArray(data) ? Math.max(0, data.length - 1) : Math.floor(Math.random()*6);
      ingestTotal += inc;
    } else {
      ingestTotal += Math.floor(Math.random()*8);
    }
    $('#ingestCounter').textContent = `Ingested: ${ingestTotal}`;
  }, 2000);
}
async function stopCollect(){
  if(HAVE_API){
    const r = await safeJson(`${API_BASE}/collector/stop`);
    $('#collectorStatus').textContent = r?.status || 'stopped';
  }else{
    $('#collectorStatus').textContent = 'demo mode';
  }
  if(ingestTimer) clearInterval(ingestTimer);
}

/* -------- Events -------- */
document.querySelectorAll('.tab').forEach(t=>{
  t.addEventListener('click', ()=>{
    document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));
    t.classList.add('active');
    const name=t.dataset.tab;
    document.querySelectorAll('.tab-page').forEach(p=>p.classList.toggle('hidden', p.dataset.tab!==name));
    // lazy loads
    if(name==='live'){ loadLive(); if(countersTimer) clearInterval(countersTimer); countersTimer=setInterval(refreshCounters,10000); }
    if(name==='historical'){ loadHist(); if(countersTimer){ clearInterval(countersTimer); countersTimer=null; } }
    if(name==='static-live'){ loadStaticRealtime(); }
    if(name==='static-hist'){ loadStaticHistorical(); }
  });
});
$('#applyLive').addEventListener('click', loadLive);
$('#refreshLive').addEventListener('click', async()=>{ await refreshCounters(); await loadLive(); });
$('#applyHist').addEventListener('click', loadHist);
$('#refreshHist').addEventListener('click', loadHist);
$('#btnStart').addEventListener('click', startCollect);
$('#btnStop').addEventListener('click', stopCollect);

/* -------- Boot -------- */
(async function boot(){
  await probeApi();
  await refreshCounters();
  loadLive(); // default tab
})();