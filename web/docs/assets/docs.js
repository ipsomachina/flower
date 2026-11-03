// Flower documentation: navigation, search, highlighting and copy
// buttons. Plain script, no dependencies, works from file://.
(function () {
  "use strict";

  var root = document.body.getAttribute("data-root") || "";

  // ---------- Mobile navigation ----------

  var menuButton = document.getElementById("menu-toggle");
  var scrim = document.getElementById("scrim");
  function setNav(open) {
    document.body.classList.toggle("nav-open", open);
    if (menuButton) {
      menuButton.setAttribute("aria-expanded", open ? "true" : "false");
      menuButton.setAttribute("aria-label", open ? "Close navigation" : "Open navigation");
    }
    if (scrim) scrim.hidden = !open;
  }
  if (menuButton) {
    menuButton.addEventListener("click", function () {
      setNav(!document.body.classList.contains("nav-open"));
    });
  }
  if (scrim) scrim.addEventListener("click", function () { setNav(false); });
  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape" && document.body.classList.contains("nav-open")) setNav(false);
  });
  var current = document.querySelector('.sidebar a[aria-current="page"]');
  if (current && current.scrollIntoView) current.scrollIntoView({ block: "center" });

  // ---------- Heading anchors ----------

  var headings = document.querySelectorAll(".content h2[id], .content h3[id]");
  Array.prototype.forEach.call(headings, function (heading) {
    var anchor = document.createElement("a");
    anchor.className = "anchor";
    anchor.href = "#" + heading.id;
    anchor.setAttribute("aria-label", "Link to this section");
    anchor.textContent = "#";
    heading.appendChild(anchor);
  });

  // ---------- Table of contents: highlight the section in view ----------

  var tocLinks = document.querySelectorAll(".toc a");
  if (tocLinks.length && "IntersectionObserver" in window) {
    var byId = {};
    Array.prototype.forEach.call(tocLinks, function (link) {
      byId[link.getAttribute("href").slice(1)] = link;
    });
    var visible = {};
    var observer = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        visible[entry.target.id] = entry.isIntersecting;
      });
      var active = null;
      for (var i = 0; i < headings.length; i++) {
        if (visible[headings[i].id]) {
          active = headings[i].id;
          break;
        }
      }
      if (!active) return;
      Array.prototype.forEach.call(tocLinks, function (link) {
        link.classList.toggle("active", link === byId[active]);
      });
    }, { rootMargin: "-56px 0px -65% 0px" });
    Array.prototype.forEach.call(headings, function (heading) {
      if (byId[heading.id]) observer.observe(heading);
    });
  }

  // ---------- Syntax highlighting ----------

  function escapeHtml(text) {
    return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  var RUST_KEYWORDS = toSet(
    "as async await break const continue crate dyn else enum extern false fn for if impl in let loop " +
    "match mod move mut pub ref return self Self static struct super trait true type unsafe use where while"
  );
  var RUST_TYPES = toSet(
    "bool char str u8 u16 u32 u64 u128 usize i8 i16 i32 i64 i128 isize f32 f64 String Vec Option Some None Ok Err Result Box"
  );
  var SHELL_COMMANDS = toSet(
    "cargo curl export rustup git cd mkdir ls cat echo set winget"
  );

  function toSet(words) {
    var set = {};
    words.split(/\s+/).forEach(function (word) { set[word] = true; });
    return set;
  }

  function rule(regex, cls) {
    return { regex: regex, cls: cls };
  }

  var GRAMMARS = {
    rust: [
      rule(/^\/\/[^\n]*/, "c"),
      rule(/^\/\*[\s\S]*?\*\//, "c"),
      rule(/^b?r#*"[\s\S]*?"#*/, "s"),
      rule(/^b?"(?:\\[\s\S]|[^"\\])*"/, "s"),
      rule(/^b?'(?:\\.|[^'\\])'/, "s"),
      rule(/^'[a-z_][a-z0-9_]*\b(?!')/, "l"),
      rule(/^#!?\[[^\]\n]*(?:\[[^\]\n]*\][^\]\n]*)*\]/, "a"),
      rule(/^[a-z_][a-zA-Z0-9_]*!/, "m"),
      rule(/^\d[\d_]*(?:\.\d[\d_]*)?(?:e[+-]?\d+)?(?:_?[iuf](?:8|16|32|64|128|size))?\b/, "n"),
      rule(/^0x[0-9a-fA-F_]+/, "n"),
      rule(/^[A-Za-z_][A-Za-z0-9_]*/, "word"),
    ],
    shell: [
      rule(/^#[^\n]*/, "c"),
      rule(/^"(?:\\[\s\S]|[^"\\])*"/, "s"),
      rule(/^'[^']*'/, "s"),
      rule(/^\$\{?[A-Za-z_][A-Za-z0-9_]*\}?/, "t"),
      rule(/^--?[A-Za-z][A-Za-z0-9-]*/, "a"),
      rule(/^\d+(?:\.\d+)?\b/, "n"),
      rule(/^[A-Za-z_][A-Za-z0-9_.-]*/, "shellword"),
    ],
    json: [
      rule(/^"(?:\\.|[^"\\])*"(?=\s*:)/, "t"),
      rule(/^"(?:\\.|[^"\\])*"/, "s"),
      rule(/^-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/, "n"),
      rule(/^(?:true|false|null)\b/, "k"),
      rule(/^\/\/[^\n]*/, "c"),
    ],
    toml: [
      rule(/^#[^\n]*/, "c"),
      rule(/^\[[^\]\n]*\]/, "a"),
      rule(/^"(?:\\.|[^"\\])*"/, "s"),
      rule(/^'[^']*'/, "s"),
      rule(/^(?:true|false)\b/, "k"),
      rule(/^\d+(?:\.\d+)?\b/, "n"),
      rule(/^[A-Za-z_][A-Za-z0-9_.-]*(?=\s*=)/, "t"),
    ],
    http: [
      rule(/^(?:GET|POST|PUT|DELETE|PATCH|HEAD)\b/, "k"),
      rule(/^HTTP\/[\d.]+/, "k"),
      rule(/^[A-Za-z][A-Za-z0-9-]*(?=:\s)/, "t"),
      rule(/^"(?:\\.|[^"\\])*"/, "s"),
      rule(/^\b\d{3}\b/, "n"),
      rule(/^(?:event|data|id|retry)(?=:)/, "a"),
    ],
  };
  GRAMMARS.bash = GRAMMARS.shell;
  GRAMMARS.sh = GRAMMARS.shell;
  GRAMMARS.console = GRAMMARS.shell;
  GRAMMARS.sse = GRAMMARS.http;

  function highlight(code, language) {
    var grammar = GRAMMARS[language];
    if (!grammar) return escapeHtml(code);
    var out = "";
    var plain = "";
    var atLineStart = true;
    function flush() {
      if (plain) {
        out += escapeHtml(plain);
        plain = "";
      }
    }
    var rest = code;
    while (rest.length) {
      var matched = false;
      for (var i = 0; i < grammar.length; i++) {
        var m = grammar[i].regex.exec(rest);
        if (!m || !m[0]) continue;
        var text = m[0];
        var cls = grammar[i].cls;
        if (cls === "word") {
          if (RUST_KEYWORDS[text]) cls = "k";
          else if (RUST_TYPES[text] || /^[A-Z][A-Za-z0-9]*$/.test(text) && /[a-z]/.test(text)) cls = "t";
          else if (/^[A-Z][A-Z0-9_]+$/.test(text)) cls = "n";
          else if (/^\s*\(/.test(rest.slice(text.length))) cls = "f";
          else cls = null;
        } else if (cls === "shellword") {
          cls = atLineStart && SHELL_COMMANDS[text] ? "k" : null;
        }
        if (cls) {
          flush();
          out += '<span class="tok-' + cls + '">' + escapeHtml(text) + "</span>";
        } else {
          plain += text;
        }
        rest = rest.slice(text.length);
        atLineStart = false;
        matched = true;
        break;
      }
      if (!matched) {
        var ch = rest[0];
        plain += ch;
        rest = rest.slice(1);
        if (ch === "\n") atLineStart = true;
        else if (ch === "|" || ch === ";" || ch === "&") atLineStart = true;
        else if (!/\s/.test(ch)) atLineStart = false;
      }
    }
    flush();
    return out;
  }

  var blocks = document.querySelectorAll(".content pre > code");
  Array.prototype.forEach.call(blocks, function (block) {
    var match = /language-([a-z]+)/.exec(block.className);
    if (match) block.innerHTML = highlight(block.textContent, match[1]);

    var pre = block.parentNode;
    var wrap = document.createElement("div");
    wrap.className = "pre-wrap";
    pre.parentNode.insertBefore(wrap, pre);
    wrap.appendChild(pre);
    var button = document.createElement("button");
    button.type = "button";
    button.className = "copy-button";
    button.textContent = "Copy";
    button.setAttribute("aria-label", "Copy code to clipboard");
    button.addEventListener("click", function () {
      var text = block.textContent.replace(/\n$/, "");
      function done(label) {
        button.textContent = label;
        setTimeout(function () { button.textContent = "Copy"; }, 1500);
      }
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(function () { done("Copied"); }, function () {
          done(fallbackCopy(text) ? "Copied" : "Press Ctrl+C");
        });
      } else {
        done(fallbackCopy(text) ? "Copied" : "Press Ctrl+C");
      }
    });
    wrap.appendChild(button);
  });

  function fallbackCopy(text) {
    var area = document.createElement("textarea");
    area.value = text;
    area.setAttribute("readonly", "");
    area.style.position = "fixed";
    area.style.opacity = "0";
    document.body.appendChild(area);
    area.select();
    var ok = false;
    try {
      ok = document.execCommand("copy");
    } catch (error) {
      ok = false;
    }
    document.body.removeChild(area);
    return ok;
  }

  // Wide tables scroll inside their own box.
  Array.prototype.forEach.call(document.querySelectorAll(".content table"), function (table) {
    if (table.parentNode.classList.contains("table-wrap")) return;
    var wrap = document.createElement("div");
    wrap.className = "table-wrap";
    table.parentNode.insertBefore(wrap, table);
    wrap.appendChild(table);
  });

  // ---------- Search ----------

  var input = document.getElementById("search-input");
  var panel = document.getElementById("search-results");
  var index = window.FLOWER_SEARCH || [];
  var selected = -1;

  function terms(query) {
    return query.toLowerCase().split(/[\s,]+/).filter(function (term) { return term.length > 0; });
  }

  function score(entry, words) {
    var title = entry.h.toLowerCase();
    var page = entry.t.toLowerCase();
    var text = entry.x.toLowerCase();
    var keys = (entry.k || "").toLowerCase();
    var total = 0;
    for (var i = 0; i < words.length; i++) {
      var word = words[i];
      var s = 0;
      if (title === word) s += 30;
      if (title.indexOf(word) === 0) s += 12;
      else if (title.indexOf(word) >= 0) s += 8;
      if (page.indexOf(word) >= 0) s += 4;
      if (keys.indexOf(word) >= 0) s += 5;
      var at = text.indexOf(word);
      if (at >= 0) s += 2 + Math.min(3, countOf(text, word));
      if (!s) return 0;
      total += s;
    }
    if (entry.d === 1) total += 1;
    return total;
  }

  function countOf(text, word) {
    var n = 0;
    var at = text.indexOf(word);
    while (at >= 0 && n < 10) {
      n++;
      at = text.indexOf(word, at + word.length);
    }
    return n;
  }

  function snippet(text, words) {
    var lower = text.toLowerCase();
    var at = -1;
    for (var i = 0; i < words.length && at < 0; i++) at = lower.indexOf(words[i]);
    var start = Math.max(0, at - 50);
    var piece = (start > 0 ? "…" : "") + text.slice(start, start + 150) + (text.length > start + 150 ? "…" : "");
    var html = escapeHtml(piece);
    words.forEach(function (word) {
      if (word.length < 2) return;
      var pattern = new RegExp("(" + word.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + ")", "gi");
      html = html.replace(pattern, "<mark>$1</mark>");
    });
    return html;
  }

  function render(query) {
    var words = terms(query);
    selected = -1;
    if (!words.length) {
      panel.hidden = true;
      panel.innerHTML = "";
      input.setAttribute("aria-expanded", "false");
      return;
    }
    var results = [];
    for (var i = 0; i < index.length; i++) {
      var s = score(index[i], words);
      if (s) results.push({ entry: index[i], score: s });
    }
    results.sort(function (a, b) { return b.score - a.score; });
    results = results.slice(0, 12);
    if (!results.length) {
      panel.innerHTML = '<div class="empty">No results for “' + escapeHtml(query) + "”</div>";
    } else {
      panel.innerHTML = results.map(function (result, i) {
        var entry = result.entry;
        var same = entry.h === entry.t;
        return '<a role="option" id="search-option-' + i + '" href="' + root + entry.u + '">' +
          '<span class="result-title">' + escapeHtml(entry.h) + "</span>" +
          (same ? "" : '<span class="result-page">' + escapeHtml(entry.t) + "</span>") +
          '<span class="result-text">' + snippet(entry.x, words) + "</span></a>";
      }).join("");
    }
    panel.hidden = false;
    input.setAttribute("aria-expanded", "true");
  }

  function move(delta) {
    var options = panel.querySelectorAll("a");
    if (!options.length) return;
    selected = (selected + delta + options.length) % options.length;
    Array.prototype.forEach.call(options, function (option, i) {
      option.setAttribute("aria-selected", i === selected ? "true" : "false");
    });
    options[selected].scrollIntoView({ block: "nearest" });
    input.setAttribute("aria-activedescendant", options[selected].id);
  }

  if (input && panel) {
    input.addEventListener("input", function () { render(input.value); });
    input.addEventListener("focus", function () { if (input.value) render(input.value); });
    input.addEventListener("keydown", function (event) {
      if (event.key === "ArrowDown") {
        event.preventDefault();
        move(1);
      } else if (event.key === "ArrowUp") {
        event.preventDefault();
        move(-1);
      } else if (event.key === "Enter") {
        var options = panel.querySelectorAll("a");
        var target = options[selected >= 0 ? selected : 0];
        if (target) {
          event.preventDefault();
          window.location.href = target.href;
        }
      } else if (event.key === "Escape") {
        input.value = "";
        render("");
        input.blur();
      }
    });
    document.addEventListener("click", function (event) {
      if (!panel.contains(event.target) && event.target !== input) panel.hidden = true;
    });
    document.addEventListener("keydown", function (event) {
      var typing = /^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement.tagName);
      if ((event.key === "/" && !typing) || (event.key === "k" && (event.ctrlKey || event.metaKey))) {
        event.preventDefault();
        input.focus();
        input.select();
      }
    });
  }
})();
