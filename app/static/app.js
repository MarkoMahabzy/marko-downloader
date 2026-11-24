const $ = id => document.getElementById(id);
let kind = 'video', info = null, analyzeTimer = null, currentJob = null;

const fmtMB = b => !b ? '0 MB' : (b/1048576 >= 1024 ? (b/1073741824).toFixed(2)+' GB' : (b/1048576).toFixed(1)+' MB');
const fmtDur = s => s ? Math.floor(s/60)+':'+String(Math.floor(s%60)).padStart(2,'0') : '';
function showErr(m){ $('errBox').textContent = m; $('errBox').style.display='block'; }
function hideErr(){ $('errBox').style.display='none'; }
function setStatus(m){ $('status').textContent = m || ''; }

$('clearBtn').onclick = () => { $('url').value=''; resetAll(); $('url').focus(); };
$('pasteBtn').onclick = async () => {
  try { $('url').value = await navigator.clipboard.readText(); onUrl(); }
  catch(e){ showErr('المتصفح مش سامح باللصق التلقائي — الصق الرابط بنفسك'); }
};
$('url').addEventListener('input', () => { clearTimeout(analyzeTimer); analyzeTimer = setTimeout(onUrl, 600); });
$('url').addEventListener('paste', () => setTimeout(onUrl, 50));

function resetAll(){ info=null; setStatus(''); hideErr();
  ['infoBox','progressBox','doneBox'].forEach(i=>$(i).style.display='none'); }

function pickKind(k){ kind=k;
  $('kindVideo').classList.toggle('sel', k==='video');
  $('kindAudio').classList.toggle('sel', k==='audio');
  fillQualities();
}
$('kindVideo').onclick = () => pickKind('video');
$('kindAudio').onclick = () => pickKind('audio');

function fillQualities(){
  const sel = $('qualitySel'); sel.innerHTML='';
  if(!info) return;
  if(info.is_direct){
    sel.innerHTML = '<option value="">الملف الأصلي كما هو'+(info.direct_size?' — '+fmtMB(info.direct_size):'')+'</option>';
    return;
  }
  if(kind==='video'){
    if(!info.video_qualities.length){ sel.innerHTML='<option value="">أفضل جودة متاحة</option>'; return; }
    info.video_qualities.forEach((q,i)=>{
      const o=document.createElement('option'); o.value=q.height;
      o.textContent = 'جودة '+q.height+'p'+(q.filesize?' — حوالي '+fmtMB(q.filesize):'');
      sel.appendChild(o);
    });
  } else {
    if(!info.audio_formats.length){ sel.innerHTML='<option value="">أفضل صوت متاح (MP3)</option>'; return; }
    info.audio_formats.forEach(a=>{
      const o=document.createElement('option'); o.value='';
      o.textContent='صوت MP3 — جودة '+a.abr+' kbps'+(a.filesize?' — حوالي '+fmtMB(a.filesize):'');
      sel.appendChild(o);
    });
  }
}

async function onUrl(){
  const url = $('url').value.trim();
  resetAll();
  if(!url) return;
  if(!/^https?:\/\//i.test(url)){ showErr('الرابط غير صالح — لازم يبدأ بـ http أو https'); return; }
  setStatus('جاري تحليل الرابط...');
  try{
    const r = await fetch('/api/analyze',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url})});
    const d = await r.json();
    if(!d.ok){ showErr(d.error||'الرابط غير مدعوم'); setStatus(''); return; }
    info = d.info; setStatus('');
    $('thumb').style.display = info.thumbnail ? 'block':'none';
    if(info.thumbnail) $('thumb').src = info.thumbnail;
    $('title').textContent = info.title || '';
    $('meta').textContent = info.duration ? ('المدة: '+fmtDur(info.duration)) : '';
    $('infoBox').style.display='block';
    pickKind('video');
  }catch(e){ showErr('حصلت مشكلة في تحليل الرابط — حاول تاني'); setStatus(''); }
}

$('goBtn').onclick = async () => {
  const url = $('url').value.trim(); hideErr();
  $('infoBox').style.display='none'; $('doneBox').style.display='none';
  $('progressBox').style.display='block';
  $('pct').textContent='0%'; $('barFill').style.width='0%';
  $('pStatus').textContent = 'جاري تجهيز الملف...';
  try{
    const q = $('qualitySel').value;
    const r = await fetch('/api/download',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({url, kind, quality: q?parseInt(q):null})});
    const d = await r.json();
    if(!d.ok){ $('progressBox').style.display='none'; $('infoBox').style.display='block'; showErr(d.error||'حصل خطأ'); return; }
    currentJob = d.job_id;
    listen(d.job_id);
  }catch(e){ showErr('حصلت مشكلة — حاول تاني'); $('progressBox').style.display='none'; }
};

function listen(jid){
  const es = new EventSource('/api/jobs/'+jid+'/events');
  es.onmessage = ev => {
    const j = JSON.parse(ev.data);
    const pct = Math.floor(j.percent||0);
    $('pct').textContent = pct+'%';
    $('barFill').style.width = pct+'%';
    $('pBytes').textContent = 'نزّل '+fmtMB(j.downloaded_bytes)+' من '+(j.total_bytes?fmtMB(j.total_bytes):'حجم غير معروف بعد');
    $('pSpeed').textContent = j.speed_bps ? ('السرعة: '+(j.speed_bps/1048576).toFixed(1)+' MB/s') : '';
    $('pEta').textContent = (j.eta_sec!=null && j.status==='downloading') ? ('المتبقي: حوالي '+j.eta_sec+' ثانية') : '';
    $('pStatus').textContent = {queued:'في الانتظار — السيرفر مشغول شوية',preparing:'جاري تجهيز الملف...',downloading:'جاري التحميل...',processing:'جاري تجهيز الملف النهائي (دمج الصوت والصورة)...',done:'اكتمل التحميل',error:'حدث خطأ'}[j.status]||j.status;
    if(j.status==='done'){ es.close(); showDone(j); }
    if(j.status==='error'){ es.close(); $('progressBox').style.display='none'; $('infoBox').style.display='block'; showErr(j.error||'حدث خطأ أثناء التحميل'); }
  };
  es.onerror = () => { /* SSE auto-reconnects; final states handled above */ };
}

function showDone(j){
  $('progressBox').style.display='none';
  $('doneBox').style.display='block';
  $('doneMeta').textContent = (j.filename||'')+' • '+fmtMB(j.filesize);
  $('dlLink').href = '/api/files/'+currentJob;
}
$('copyBtn').onclick = async () => {
  const link = location.origin + '/api/files/' + currentJob;
  try{ await navigator.clipboard.writeText(link); $('copyBtn').textContent='اتنسخ ✓'; }
  catch(e){ prompt('انسخ الرابط ده:', link); }
};
$('againBtn').onclick = () => { $('url').value=''; resetAll(); $('url').focus(); window.scrollTo({top:0,behavior:'smooth'}); };
