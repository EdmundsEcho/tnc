/**
 * FileViewer — modal dialog that fetches a TOML or JSON file and renders it
 * with lightweight syntax highlighting. Format is inferred from the file
 * extension (or can be passed explicitly via `format`).
 */
import React, { useEffect, useState } from 'react';

export default function FileViewer({ url, title, format, onClose, headerExtras }) {
  const [text, setText] = useState(null);
  const [error, setError] = useState(null);

  const fmt = format || inferFormat(url);

  useEffect(() => {
    if (!url) return;
    let cancelled = false;
    fetch(url)
      .then(r => {
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
        return r.text();
      })
      .then(t => { if (!cancelled) setText(t); })
      .catch(e => { if (!cancelled) setError(e.message); });
    return () => { cancelled = true; };
  }, [url]);

  useEffect(() => {
    const onKey = e => { if (e.key === 'Escape') onClose?.(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const pretty = pretty_(text, fmt);
  const highlighted = highlight(pretty, fmt);

  return (
    <div style={styles.backdrop} onClick={onClose}>
      <div style={styles.modal} onClick={e => e.stopPropagation()}>
        <header style={styles.header}>
          <div>
            <div style={styles.title}>{title || url}</div>
            <div style={styles.subtitle}>{fmt.toUpperCase()} · {url}</div>
          </div>
          <div style={styles.actions}>
            {headerExtras}
            <a href={url} download style={styles.downloadBtn}>Download</a>
            <button onClick={onClose} style={styles.closeBtn} aria-label="Close">×</button>
          </div>
        </header>
        <div style={styles.body}>
          {error && <div style={styles.error}>Failed to load: <code>{error}</code></div>}
          {!error && text === null && <div style={styles.loading}>Loading…</div>}
          {!error && text !== null && (
            <pre style={styles.pre}>
              <code dangerouslySetInnerHTML={{ __html: highlighted }} />
            </pre>
          )}
        </div>
      </div>
    </div>
  );
}

function inferFormat(url) {
  if (!url) return 'text';
  const u = url.toLowerCase();
  if (u.endsWith('.toml')) return 'toml';
  if (u.endsWith('.json')) return 'json';
  return 'text';
}

function pretty_(text, fmt) {
  if (text == null) return '';
  if (fmt === 'json') {
    try { return JSON.stringify(JSON.parse(text), null, 2); }
    catch { return text; }
  }
  return text;
}

// ─────────────────────────────────────────────────────────────
// Lightweight highlighter — HTML-escape first, then wrap tokens.
// Order matters: do comments/strings first so later rules don't
// clobber content inside them.
// ─────────────────────────────────────────────────────────────
const C = {
  comment: '#6b7280',
  string:  '#047857',
  number:  '#1d4ed8',
  bool:    '#b45309',
  key:     '#7c3aed',
  section: '#b91c1c',
  punct:   '#6b7280',
};

function escapeHtml(s) {
  return s
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;');
}

function highlight(text, fmt) {
  const esc = escapeHtml(text);
  if (fmt === 'json') return highlightJson(esc);
  if (fmt === 'toml') return highlightToml(esc);
  return esc;
}

function wrap(color, content) {
  return `<span style="color:${color}">${content}</span>`;
}

function highlightJson(esc) {
  // strings (including keys — we distinguish keys by the trailing colon)
  // numbers, booleans, null
  return esc.replace(
    /("(?:\\.|[^"\\])*")(\s*:)?|\b(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\b|\b(true|false|null)\b/g,
    (_, str, colon, num, kw) => {
      if (str && colon) return wrap(C.key, str) + wrap(C.punct, colon);
      if (str) return wrap(C.string, str);
      if (num) return wrap(C.number, num);
      if (kw === 'true' || kw === 'false') return wrap(C.bool, kw);
      if (kw === 'null') return wrap(C.punct, kw);
      return _;
    }
  );
}

function highlightToml(esc) {
  // Process line-by-line so comments stop at end-of-line.
  return esc.split('\n').map(line => {
    // Comment: everything from the first unquoted # to end of line.
    const commentIdx = findCommentStart(line);
    let main = commentIdx >= 0 ? line.slice(0, commentIdx) : line;
    const comment = commentIdx >= 0 ? line.slice(commentIdx) : '';

    // [section] or [[array-of-tables]]
    main = main.replace(/^(\s*)(\[\[?[^\]]+\]\]?)/,
      (_, ws, sec) => ws + wrap(C.section, sec));

    // Strings (double-quoted)
    main = main.replace(/"(?:\\.|[^"\\])*"/g,
      m => wrap(C.string, m));

    // Key = value — catch bare/quoted keys before the =
    main = main.replace(/^(\s*)([A-Za-z0-9_.-]+|"(?:\\.|[^"\\])*")(\s*)(=)/,
      (_, ws, key, sp, eq) => ws + wrap(C.key, key) + sp + wrap(C.punct, eq));

    // Numbers (floats, ints, scientific). Avoid matching inside already-wrapped spans.
    main = main.replace(/\b(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\b/g,
      (m, _p1, offset, whole) => {
        // Skip if we're inside an existing <span>…</span>
        if (insideSpan(whole, offset)) return m;
        return wrap(C.number, m);
      });

    // Booleans
    main = main.replace(/\b(true|false)\b/g,
      (m, _p1, offset, whole) => insideSpan(whole, offset) ? m : wrap(C.bool, m));

    return main + (comment ? wrap(C.comment, comment) : '');
  }).join('\n');
}

function findCommentStart(line) {
  let inStr = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"' && line[i - 1] !== '\\') inStr = !inStr;
    if (!inStr && ch === '#') return i;
  }
  return -1;
}

function insideSpan(whole, offset) {
  // True if offset falls inside a <span …>…</span> we inserted earlier.
  const before = whole.slice(0, offset);
  const opens = (before.match(/<span /g) || []).length;
  const closes = (before.match(/<\/span>/g) || []).length;
  return opens > closes;
}

const styles = {
  backdrop: {
    position: 'fixed', inset: 0, background: 'rgba(17, 24, 39, 0.55)',
    display: 'flex', alignItems: 'center', justifyContent: 'center',
    zIndex: 50, padding: 24,
  },
  modal: {
    background: '#fff', borderRadius: 8,
    width: 'min(960px, 100%)', maxHeight: '85vh',
    display: 'flex', flexDirection: 'column',
    boxShadow: '0 10px 32px rgba(0,0,0,0.2)',
    overflow: 'hidden',
  },
  header: {
    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
    padding: '12px 20px', borderBottom: '1px solid #e5e7eb',
    background: '#f9fafb',
  },
  title: {
    fontFamily: 'system-ui', fontSize: 14, fontWeight: 600, color: '#111827',
  },
  subtitle: {
    fontFamily: 'system-ui', fontSize: 11, color: '#6b7280', marginTop: 2,
  },
  actions: { display: 'flex', gap: 8, alignItems: 'center' },
  downloadBtn: {
    fontSize: 12, padding: '4px 10px',
    border: '1px solid #d1d5db', borderRadius: 4,
    color: '#374151', textDecoration: 'none', background: '#fff',
  },
  closeBtn: {
    fontSize: 20, lineHeight: 1, width: 28, height: 28,
    border: 'none', background: 'transparent', cursor: 'pointer',
    color: '#6b7280',
  },
  body: {
    overflow: 'auto', padding: 0,
  },
  pre: {
    margin: 0, padding: 20,
    fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
    fontSize: 12, lineHeight: 1.55,
    whiteSpace: 'pre',
    color: '#111827',
    background: '#fff',
  },
  loading: { padding: 24, color: '#6b7280', textAlign: 'center' },
  error: {
    padding: 20, margin: 16, borderRadius: 4,
    background: '#fef2f2', color: '#b91c1c', fontSize: 13,
  },
};
