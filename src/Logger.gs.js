// Logger.gs — leveled logger for Apps Script with module tags + timers
var LoggerEx = (function () {
  var LEVELS = { ERROR: 0, WARN: 1, INFO: 2, DEBUG: 3 };
  var current = LEVELS.INFO; // default: INFO/WARN/ERROR

  function fmtArgs(args) {
    return Array.prototype.map.call(args, function (x) {
      try {
        if (x instanceof Error) return x.stack || (x.name + ': ' + x.message);
        if (x === null || x === undefined) return String(x);
        if (Object.prototype.toString.call(x) === '[object Date]') return String(x); // prints in project tz
        return (typeof x === 'object') ? JSON.stringify(x) : String(x);
      } catch (_) { return String(x); }
    }).join(' ');
  }

  function tsParts() {
    var now = new Date();
    // project-local and UTC timestamps
    var local = Utilities.formatDate(now, Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss");
    var utc   = Utilities.formatDate(now, "UTC", "yyyy-MM-dd'T'HH:mm:ss'Z'");
    return { local: local, utc: utc };
  }

  function out(levelName, levelVal, args, modTag) {
    if (levelVal > current) return;
    var msg = fmtArgs(args);
    var ts = tsParts();
    var prefix = '[' + levelName + ']' + (modTag ? '[' + modTag + ']' : '')
               + ' ' + ts.local + ' | ' + ts.utc + ' — ';
    Logger.log(prefix + msg);
    // Optional V8 console mirroring:
  //  if (levelName === 'ERROR') console.error(prefix + msg);
  //  else if (levelName === 'WARN') console.warn(prefix + msg);
  //  else console.log(prefix + msg);
  }

  function makeTagged(modTag) {
    return {
      setLevel: function (levelName) { current = LEVELS[String(levelName).toUpperCase()] || current; },
      info:  function () { out('INFO',  LEVELS.INFO,  arguments, modTag); }, 
      log:   function () { out('INFO',  LEVELS.INFO,  arguments, modTag); },
      warn:  function () { out('WARN',  LEVELS.WARN,  arguments, modTag); },
      error: function () { out('ERROR', LEVELS.ERROR, arguments, modTag); },
      debug: function () { out('DEBUG', LEVELS.DEBUG, arguments, modTag); },
      /** start a simple timer; use t.stamp("label") to log +Δms */
      startTimer: function (label) {
        var t0 = Date.now();
        return {
          stamp: function () {
            var delta = Date.now() - t0;
            var args = Array.prototype.slice.call(arguments);
            args.unshift(label + ' +' + delta + 'ms');
            out('DEBUG', LEVELS.DEBUG, args, modTag);
          }
        };
      }
    };
  }

  return {
    setLevel: function (levelName) { current = LEVELS[String(levelName).toUpperCase()] || current; },
    info:  function () { out('INFO',  LEVELS.INFO,  arguments); }, // ← add
    log:   function () { out('INFO',  LEVELS.INFO,  arguments); },
    warn:  function () { out('WARN',  LEVELS.WARN,  arguments); },
    error: function () { out('ERROR', LEVELS.ERROR, arguments); },
    debug: function () { out('DEBUG', LEVELS.DEBUG, arguments); },
    /** get a module-scoped logger: const L = LoggerEx.withTag('History') */
    withTag: function (modTag) { return makeTagged(modTag); }
  };
})();