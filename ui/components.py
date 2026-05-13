"""Stateless HTML renderers used by the Scraper tab.

render_log(entries) → string of HTML <div> rows for the Live Processing Log,
                      keyed by event status (start / ok / priority / blocked /
                      err / dead / retry).
rmetric(label, value, color, help) → metric tile HTML for the Live Progress
                      row. Optional `help` renders a (?) glyph that shows
                      the help text as a browser-native tooltip on hover.

Both return strings; the caller is responsible for passing them to
st.markdown(..., unsafe_allow_html=True). No Streamlit import is needed
here because nothing renders directly — only HTML is produced.
"""
from html import escape


def render_log(entries):
    lines = ""
    for e in entries:
        b, s, st2, n = e.get("bot", "?"), e.get("stock", ""), e.get("status", ""), e.get("num", "")
        if st2 == "start":
            lines += f'<div class="ll"><span class="bi">[Bot {b}]</span> #{n} &#8594; Scraping <span class="sn">{s}</span>...</div>'
        elif st2 == "ok":
            lines += f'<div class="ll"><span class="bi">[Bot {b}]</span> #{n} &#8594; <span class="lok">Done</span></div>'
        elif st2 == "priority":
            lines += f'<div class="ll"><span class="bi">[Bot {b}]</span> #{n} &#8594; <span class="lpr">PRIORITY</span></div>'
        elif st2 == "blocked":
            lines += f'<div class="ll"><span class="bi">[Bot {b}]</span> #{n} &#8594; <span class="lbl">BLACKLISTED</span></div>'
        elif st2 == "err":
            lines += f'<div class="ll"><span class="bi">[Bot {b}]</span> #{n} &#8594; <span class="ler">Error</span></div>'
        elif st2 == "dead":
            lines += f'<div class="ll"><span class="bi">[Bot {b}]</span> <span class="ler">CRASHED: {s} &#8212; restarting</span></div>'
        elif st2 == "retry":
            lines += f'<div class="ll"><span class="bi">[Bot {b}]</span> #{n} &#8594; <span class="lpr">RETRY</span> <span class="sn">{s}</span></div>'
    return f'<div class="llog"><div class="lt">Live Processing Log</div>{lines}</div>'


def rmetric(label, value, color="g", help=None):
    cmap = {"g": "cv-green", "b": "cv-blue", "r": "cv-red", "p": "cv-purple"}
    help_glyph = (
        f' <span style="opacity:0.5;font-size:0.75em;cursor:help;margin-left:0.25rem;" '
        f'title="{escape(str(help), quote=True)}">(?)</span>'
        if help else ''
    )
    return f'<div class="mc {color}"><div class="mv {cmap.get(color, "cv-green")}">{value}</div><div class="ml">{label}{help_glyph}</div></div>'
