// LoggerEx.gs — leveled logger with module tags + timers (GAS V8-safe)
var LoggerEx = (function () {
  var LEVELS = { ERROR:0, WARN:1, INFO:2, DEBUG:3 };
  var current = LEVELS.INFO;
  var timers = Object.create(null);

  function levelName(n){ for (var k in LEVELS) if (LEVELS[k]===n) return k; return String(n); }
  function ts() {
    var now = new Date();
    var local = Utilities.formatDate(now, Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
    var utc   = Utilities.formatDate(now, 'UTC', "yyyy-MM-dd'T'HH:mm:ss'Z'");
    return local + ' | ' + utc;
  }
  function fmt(x){
    try{
      if (x instanceof Error) return x.stack || (x.name+': '+x.message);
      if (x === null || x === undefined) return String(x);
      if (x instanceof Date) return Utilities.formatDate(x, Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
      if (typeof x === 'object'){ var s = JSON.stringify(x); return s.length>2000 ? s.slice(0,2000)+'…' : s; }
      var s = String(x); return s.length>2000 ? s.slice(0,2000)+'…' : s;
    } catch(_) { return String(x); }
  }
  function out(level, mod, args){
    if (level > current) return;
    var head = '['+levelName(level)+']' + (mod ? '['+mod+']' : '');
    var line = head + ' ' + ts() + ' — ' + Array.prototype.map.call(args, fmt).join(' ');
    console.log(line);
    Logger.log(line);
  }
  function make(mod){
    return {
      error: function(){ out(LEVELS.ERROR, mod, arguments); },
      warn:  function(){ out(LEVELS.WARN,  mod, arguments); },
      info:  function(){ out(LEVELS.INFO,  mod, arguments); },
      debug: function(){ out(LEVELS.DEBUG, mod, arguments); },
      time: function(key){ timers[mod+'::'+key] = Date.now(); },
      timeEnd: function(key){
        var id = mod+'::'+key, t0 = timers[id];
        if (t0){ out(LEVELS.INFO, mod, ['⏱ '+key+': '+(Date.now()-t0)+'ms']); delete timers[id]; }
      }
    };
  }

  return {
    LEVELS: LEVELS,
    setLevel: function(l){
      current = (typeof l==='string') ? (LEVELS[l.toUpperCase()] ?? current)
               : (typeof l==='number' ? l : current);
      out(LEVELS.INFO, 'LoggerEx', ['level set to', levelName(current)]);
    },
    getLevel: function(){ return current; },
    tag: make,                              // ← the method you’re missing
    error: function(){ out(LEVELS.ERROR, '', arguments); },
    warn:  function(){ out(LEVELS.WARN,  '', arguments); },
    info:  function(){ out(LEVELS.INFO,  '', arguments); },
    debug: function(){ out(LEVELS.DEBUG, '', arguments); }
  };
})();
