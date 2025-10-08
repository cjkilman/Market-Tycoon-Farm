/**
 * ProjectTime (PT) — core helpers centralized in one place
 * - Single source of truth for timezone-aware operations
 * - Replaces scattered duplicates in feature modules (e.g., HistoryManager)
 * - All functions rely on the Apps Script project time zone (appsscript.json)
 */
var PT = (function(){
  'use strict';

  // --- Basic primitives ---
  function tz(){
    try { return Session.getScriptTimeZone() || 'UTC'; } catch (e) { return 'UTC'; }
  }
  function now(){ return new Date(); } // Date objects are local to project tz when formatted

  // Build a Date at project-local calendar components
  function projectDate(y, mZeroBased, d, h, mi, s){
    return new Date(y, mZeroBased, d, h||0, mi||0, s||0, 0);
  }

  // Today at local H:M:S
  function todayAt(h, mi, s){
    const n = now();
    return projectDate(n.getFullYear(), n.getMonth(), n.getDate(), h||0, mi||0, s||0);
  }

  // Convert a UTC hour:minute (EVE server time) to a project-local Date for "today"
  function utcHMToProjectToday(hm, ref){
    const n = ref || now();
    return new Date(Date.UTC(n.getUTCFullYear(), n.getUTCMonth(), n.getUTCDate(), hm.h||0, hm.m||0, 0, 0));
  }

  // In ProjectTime (PT).gs, replace the coerceHM definition with this:

function coerceHM(raw){
  // Accept legacy array shape [h, m]
  if (Array.isArray(raw) && raw.length >= 2) {
    var hA = parseInt(raw[0], 10); var mA = parseInt(raw[1], 10);
    var h = Math.max(0, Math.min(23, isFinite(hA) ? hA : 0));
    var m = Math.max(0, Math.min(59, isFinite(mA) ? mA : 0));
    return { h: h, m: m };
  }
  if (raw instanceof Date && !isNaN(raw)) return { h: raw.getHours(), m: raw.getMinutes() };
  if (typeof raw === 'number' && isFinite(raw)) return { h: Math.max(0, Math.min(23, Math.floor(raw/100))), m: Math.max(0, Math.min(59, raw%100)) };

  const s = String(raw||'').trim().toLowerCase()
    .replace(/\butc\b|\beve\b|z/g,'')
    .replace(/\s+/g,' ')
    .trim();

  // ❌ avoid optional-chaining on array index
  const amMatch = /(am|pm)\b/.exec(s);
  const ampm = amMatch ? amMatch[1] : null;

  const nums = s.replace(/[^0-9:.\s-]/g, ' ').trim();

  // allow "11:30", "1130", "11"
  let m1 = /^(\d{1,2})[:.\- ]?(\d{2})?$/.exec(nums);
  if (!m1){
    const m2 = /^(\d{3,4})$/.exec(nums.replace(/\s+/g,''));
    if (m2){
      const n = parseInt(m2[1],10);
      return applyAmPm({h:Math.floor(n/100), m:n%100}, ampm);
    }
    const m3 = /^(\d{1,2})$/.exec(nums);
    if (m3) return applyAmPm({h:parseInt(m3[1],10), m:0}, ampm);
    return {h:11, m:0};
  }
  return applyAmPm({
    h: parseInt(m1[1],10),
    m: (m1[2]!==undefined ? parseInt(m1[2],10) : 0)
  }, ampm);
}

  function applyAmPm(hm, ampm){
    let h = hm.h|0, m = hm.m|0;
    if (ampm==='am'){ if (h===12) h=0; } else if (ampm==='pm'){ if (h<12) h+=12; }
    return { h: Math.max(0,Math.min(23,h)), m: Math.max(0,Math.min(59,m)) };
  }

  // Date parsing that accepts Date, number, ISO-ish strings
  function parseDateSafe(v){
    if (v instanceof Date && !isNaN(v)) return v;
    if (typeof v === 'number' && isFinite(v)) { const d = new Date(v); if (!isNaN(d)) return d; }
    if (typeof v === 'string'){
      const d1 = new Date(v); if (!isNaN(d1)) return d1;
      const m = v.match(/^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?$/);
      if (m){ const [,Y,Mo,D,H='00',Mi='00',S='00'] = m; const d2 = new Date(+Y, +Mo-1, +D, +H, +Mi, +S); if (!isNaN(d2)) return d2; }
    }
    return new Date('Invalid');
  }

  // Format helpers
  function yyyymmdd(d){ const x = (d instanceof Date)? d : new Date(d); return `${x.getFullYear()}-${('0'+(x.getMonth()+1)).slice(-2)}-${('0'+x.getDate()).slice(-2)}`; }
  
  // Simple predicates
  const isNil   = v => v===null || v===undefined;
  const isBlank = v => isNil(v) || v==='';
  const hasPos  = v => { const n = (typeof v==='number')? v : Number(v); return isFinite(n) && n>0; };

  return {
    tz, now, projectDate, todayAt, utcHMToProjectToday, coerceHM,
    parseDateSafe, yyyymmdd, isNil, isBlank, hasPos
  };
})();
