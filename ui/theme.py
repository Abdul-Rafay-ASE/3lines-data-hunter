"""Theme palette + Streamlit CSS injection for the dark/light toggle.

The application has 22 named colors plus a few inline ternary fallbacks
that depend on `is_dark`. `apply_theme(is_dark)` does two jobs:

  1. Builds the colors dict for the requested theme.
  2. Injects the full CSS via st.markdown (525+ lines of styling for
     metric cards, tabs, banners, buttons, scrollbars, etc.).

Returns the colors dict so callers can unpack named colors for inline
use in subsequent st.markdown calls (e.g. status pills that color
green/red based on a flag).
"""
import streamlit as st


def apply_theme(is_dark):
    if is_dark:
        _bg="#0a0e1a"; _card="rgba(15,23,42,0.7)"; _card_solid="#0f172a"; _border="rgba(30,41,59,0.6)"; _border2="#334155"
        _text="#f1f5f9"; _text2="#cbd5e1"; _muted="#64748b"; _input="#0f172a"
        _accent="#3b82f6"; _accent2="#60a5fa"
        _green="#10b981"; _green2="#34d399"; _red="#ef4444"; _red2="#f87171"
        _yellow="#f59e0b"; _yellow2="#fbbf24"; _purple="#8b5cf6"; _purple2="#a78bfa"
        _glass_bg="rgba(15,23,42,0.6)"; _glass_border="rgba(59,130,246,0.15)"
        _shadow="rgba(0,0,0,0.4)"
    else:
        _bg="#f0f4f8"; _card="rgba(255,255,255,0.75)"; _card_solid="#ffffff"; _border="rgba(226,232,240,0.8)"; _border2="#cbd5e1"
        _text="#0f172a"; _text2="#334155"; _muted="#64748b"; _input="#ffffff"
        _accent="#2563eb"; _accent2="#3b82f6"
        _green="#059669"; _green2="#10b981"; _red="#dc2626"; _red2="#ef4444"
        _yellow="#d97706"; _yellow2="#f59e0b"; _purple="#7c3aed"; _purple2="#8b5cf6"
        _glass_bg="rgba(255,255,255,0.6)"; _glass_border="rgba(37,99,235,0.12)"
        _shadow="rgba(0,0,0,0.06)"

    st.markdown(f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&display=swap');

    /* ── Keyframes ── */
    @keyframes gradientShift {{
        0% {{ background-position: 0% 50%; }}
        50% {{ background-position: 100% 50%; }}
        100% {{ background-position: 0% 50%; }}
    }}
    @keyframes blink {{ 0%,100%{{opacity:1}} 50%{{opacity:.2}} }}
    @keyframes fadeInUp {{
        from {{ opacity: 0; transform: translateY(12px); }}
        to {{ opacity: 1; transform: translateY(0); }}
    }}
    @keyframes pulseGlow {{
        0%,100% {{ box-shadow: 0 0 5px rgba(16,185,129,0.3); }}
        50% {{ box-shadow: 0 0 20px rgba(16,185,129,0.5); }}
    }}
    @keyframes shimmer {{
        0% {{ background-position: -200% 0; }}
        100% {{ background-position: 200% 0; }}
    }}

    /* ── Base ── */
    .stApp {{
        background: {_bg} !important;
        font-family: 'Inter', sans-serif;
    }}
    section[data-testid="stSidebar"], #MainMenu, footer, header {{ display: none !important; }}
    .stApp, .stApp p, .stApp span, .stApp label, .stApp div,
    .stApp li, .stApp h1, .stApp h2, .stApp h3, .stApp h4,
    .stApp summary, .stApp td, .stApp th, .stApp a,
    .stApp strong, .stApp em, .stApp code {{ color: {_text} !important; }}

    /* ── Premium Header ── */
    .elite-header {{
        background: linear-gradient(135deg, #0a1628 0%, #0f2340 25%, #132d5e 50%, #0f2340 75%, #0a1628 100%);
        background-size: 300% 300%;
        animation: gradientShift 8s ease infinite;
        padding: 1.2rem 2.2rem;
        margin: -1rem -1rem 0 -1rem;
        display: flex;
        align-items: center;
        justify-content: space-between;
        position: relative;
        overflow: hidden;
        border-bottom: 1px solid rgba(59,130,246,0.2);
    }}
    .elite-header::before {{
        content: '';
        position: absolute;
        top: 0; left: -50%; width: 200%; height: 100%;
        background: linear-gradient(90deg, transparent, rgba(59,130,246,0.03), transparent);
        animation: shimmer 6s linear infinite;
    }}
    .elite-header::after {{
        content: '';
        position: absolute;
        bottom: 0; left: 0; right: 0; height: 2px;
        background: linear-gradient(90deg, transparent 0%, {_accent} 20%, {_accent2} 50%, {_accent} 80%, transparent 100%);
        opacity: 0.6;
    }}
    .eh-brand {{
        display: flex; align-items: center; gap: 16px; z-index: 1;
    }}
    .eh-logo {{
        font-size: 1.7rem; font-weight: 900; color: #fff !important;
        letter-spacing: -0.5px;
        text-shadow: 0 0 30px rgba(59,130,246,0.3);
    }}
    .eh-logo b {{ color: #60a5fa !important; font-weight: 400; }}
    .eh-sep {{ width: 1px; height: 28px; background: rgba(255,255,255,0.1); }}
    .eh-sub {{ font-size: 0.72rem; color: rgba(180,200,230,0.6) !important; font-weight: 500; letter-spacing: 0.5px; }}
    .eh-right {{ display: flex; align-items: center; gap: 14px; z-index: 1; }}
    .eh-pill {{
        display: inline-flex; align-items: center; gap: 7px;
        background: rgba(16,185,129,0.08);
        border: 1px solid rgba(16,185,129,0.2);
        backdrop-filter: blur(10px);
        padding: 6px 16px; border-radius: 99px;
        font-size: 0.68rem; font-weight: 600; color: #34d399 !important;
        font-family: 'JetBrains Mono', monospace;
        animation: pulseGlow 3s ease-in-out infinite;
    }}
    .eh-pill .dot {{
        width: 6px; height: 6px; border-radius: 50%; background: #34d399;
        animation: blink 2s infinite;
    }}
    .eh-ver {{ font-size: 0.58rem; color: rgba(120,150,190,0.4) !important;
        font-family: 'JetBrains Mono', monospace; }}

    /* ── Streamlit native tabs override ── */
    .stTabs [data-baseweb="tab-list"] {{
        background: {_glass_bg} !important;
        backdrop-filter: blur(12px);
        border-bottom: 1px solid {_border} !important;
        gap: 0 !important;
        padding: 0 0.5rem !important;
        border-radius: 12px 12px 0 0;
    }}
    .stTabs [data-baseweb="tab"] {{
        font-family: 'Inter', sans-serif !important;
        font-weight: 700 !important;
        font-size: 0.85rem !important;
        color: {_muted} !important;
        padding: 0.9rem 1.8rem !important;
        border-bottom: 2px solid transparent !important;
        background: transparent !important;
        transition: all 0.3s ease !important;
        letter-spacing: 0.3px !important;
    }}
    .stTabs [data-baseweb="tab"]:hover {{
        color: {_text} !important;
        background: rgba(59,130,246,0.05) !important;
    }}
    .stTabs [aria-selected="true"] {{
        color: {_accent2} !important;
        border-bottom: 2.5px solid {_accent2} !important;
        background: transparent !important;
    }}
    .stTabs [data-baseweb="tab-highlight"] {{
        background: {_accent2} !important;
    }}
    .stTabs [data-baseweb="tab-border"] {{
        display: none !important;
    }}

    /* ── Metric Cards (Premium Glass) ── */
    .mc {{
        background: {_glass_bg};
        backdrop-filter: blur(12px);
        border: 1px solid {_glass_border};
        border-radius: 16px; padding: 1.3rem 1rem;
        text-align: center; position: relative; overflow: hidden;
        transition: all 0.3s cubic-bezier(0.4,0,0.2,1);
        animation: fadeInUp 0.5s ease-out;
    }}
    .mc:hover {{
        transform: translateY(-4px);
        box-shadow: 0 12px 40px {_shadow};
        border-color: rgba(59,130,246,0.3);
    }}
    .mc::before {{
        content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px;
    }}
    .mc.g::before {{ background: linear-gradient(90deg,{_green},{_green2}); }}
    .mc.b::before {{ background: linear-gradient(90deg,{_accent},{_accent2}); }}
    .mc.r::before {{ background: linear-gradient(90deg,{_red},{_red2}); }}
    .mc.p::before {{ background: linear-gradient(90deg,{_purple},{_purple2}); }}
    .mc .mv {{
        font-size: 1.8rem; font-weight: 800; line-height: 1.2; margin-top: 4px;
        font-family: 'JetBrains Mono', monospace;
    }}
    .mc .ml {{
        font-size: 0.6rem; text-transform: uppercase; letter-spacing: 2.5px;
        color: {_muted} !important; margin-top: 8px; font-weight: 700;
    }}
    .cv-green {{ color: {_green2} !important; }}
    .cv-blue  {{ color: {_accent2} !important; }}
    .cv-red   {{ color: {_red2} !important; }}
    .cv-purple{{ color: {_purple2} !important; }}

    /* ── Stat Grid ── */
    .stat-grid {{
        display: grid; grid-template-columns: repeat(auto-fit, minmax(155px,1fr));
        gap: 0.8rem; margin: 0.8rem 0;
    }}
    .stat-cell {{
        background: {_glass_bg};
        backdrop-filter: blur(10px);
        border: 1px solid {_glass_border}; border-radius: 12px;
        padding: 1rem 1.1rem; display: flex; justify-content: space-between; align-items: center;
        transition: all 0.25s ease;
    }}
    .stat-cell:hover {{
        transform: translateY(-2px);
        box-shadow: 0 8px 24px {_shadow};
    }}
    .stat-cell .sl {{ font-size: 0.72rem; color: {_muted} !important; font-weight: 600; }}
    .stat-cell .sv {{
        font-size: 0.88rem; font-weight: 700;
        font-family: 'JetBrains Mono', monospace;
    }}

    /* ── Section Divider ── */
    .sec {{
        font-size: 0.72rem; font-weight: 800; color: {_muted} !important;
        text-transform: uppercase; letter-spacing: 2.5px;
        margin: 1.4rem 0 0.7rem; display: flex; align-items: center; gap: 10px;
    }}
    .sec::after {{ content: ''; flex: 1; height: 1px; background: linear-gradient(90deg, {_border}, transparent); }}

    /* ── Step Cards ── */
    .step-card {{
        background: {_glass_bg};
        backdrop-filter: blur(12px);
        border: 1px solid {_glass_border};
        border-radius: 16px;
        padding: 1.6rem 1.2rem;
        text-align: center;
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
    }}
    .step-card:hover {{
        transform: translateY(-3px);
        box-shadow: 0 10px 30px {_shadow};
        border-color: {_accent};
    }}
    .step-card.active {{
        border-color: {_accent};
        box-shadow: 0 0 20px rgba(59,130,246,0.15);
    }}
    .step-card.done {{
        border-color: {_green};
        box-shadow: 0 0 15px rgba(16,185,129,0.12);
    }}
    .step-num {{
        display: inline-flex; align-items: center; justify-content: center;
        width: 36px; height: 36px; border-radius: 50%;
        background: linear-gradient(135deg, {_accent}, {_accent2});
        color: #fff !important; font-weight: 800; font-size: 0.9rem;
        margin-bottom: 0.7rem;
    }}
    .step-num.done {{
        background: linear-gradient(135deg, {_green}, {_green2});
    }}
    .step-icon {{ font-size: 2rem; margin-bottom: 0.5rem; display: block; }}
    .step-title {{ font-size: 0.9rem; font-weight: 700; color: {_text} !important; margin-bottom: 0.3rem; }}
    .step-desc {{ font-size: 0.72rem; color: {_muted} !important; line-height: 1.5; }}

    /* ── Inputs ── */
    div[data-testid="stTextInput"] input,
    div[data-testid="stNumberInput"] input,
    div[data-testid="stTextArea"] textarea {{
        background: {_input} !important; color: {_text} !important;
        -webkit-text-fill-color: {_text} !important;
        border: 1px solid {_border} !important; border-radius: 10px !important;
        font-weight: 600 !important; font-size: 0.92rem !important;
        transition: all 0.25s ease !important;
    }}
    div[data-testid="stTextInput"] input:focus,
    div[data-testid="stNumberInput"] input:focus {{
        border-color: {_accent2} !important;
        box-shadow: 0 0 0 3px rgba(59,130,246,0.12) !important;
    }}
    div[data-testid="stNumberInput"] button {{
        color: {_text} !important; background: {_card_solid} !important;
        border: 1px solid {_border} !important;
    }}
    .stApp label, div[data-testid="stWidgetLabel"] label,
    div[data-testid="stWidgetLabel"] p {{
        color: {_text2} !important; font-weight: 700 !important; font-size: 0.82rem !important;
    }}
    .stApp .stCaption, .stApp small {{ color: {_muted} !important; }}
    .stApp input::placeholder, .stApp textarea::placeholder {{
        color: {_muted} !important; opacity: 0.6 !important;
    }}

    /* ── Selectbox / Popover ── */
    div[data-testid="stSelectbox"] > div > div {{
        background: {_input} !important; border: 1px solid {_border} !important; border-radius: 10px !important;
    }}
    div[data-testid="stSelectbox"] span {{ color: {_text} !important; }}
    [data-baseweb="popover"] {{ background: {_input} !important; border: 1px solid {_border} !important; }}
    [data-baseweb="popover"] ul {{ background: {_input} !important; }}
    [data-baseweb="popover"] li, [data-baseweb="menu"] li, ul[role="listbox"] li {{
        background: {_input} !important; color: {_text} !important;
    }}
    [data-baseweb="popover"] li:hover, [data-baseweb="menu"] li:hover, ul[role="listbox"] li:hover {{
        background: {_card_solid} !important;
    }}
    ul[role="listbox"] {{ background: {_input} !important; }}

    /* ── File Uploader (Premium Drop Zone) ── */
    div[data-testid="stFileUploader"] > div {{
        background: {_glass_bg} !important;
        border: 2px dashed {_accent} !important;
        border-radius: 16px !important;
        transition: all 0.3s ease !important;
        padding: 1rem !important;
    }}
    div[data-testid="stFileUploader"] > div:hover {{
        border-color: {_green} !important;
        background: {'rgba(16,185,129,0.05)' if is_dark else 'rgba(16,185,129,0.03)'} !important;
        box-shadow: 0 0 30px rgba(16,185,129,0.1) !important;
    }}
    div[data-testid="stFileUploader"] span, div[data-testid="stFileUploader"] small,
    div[data-testid="stFileUploader"] p, div[data-testid="stFileUploader"] div {{ color: {_text2} !important; }}
    div[data-testid="stFileUploader"] button {{
        color: {_text} !important; background: {_card_solid} !important; border: 1px solid {_border} !important;
    }}

    /* ── Expander ── */
    div[data-testid="stExpander"] {{
        background: {_card_solid} !important; border: 1px solid {_border} !important; border-radius: 12px !important;
    }}
    div[data-testid="stExpander"] details summary {{ color: {_text2} !important; font-weight: 700 !important; }}

    /* ── Alert ── */
    .stAlert, div[data-testid="stAlert"] {{
        background: {_card_solid} !important; border-color: {_border} !important; border-radius: 10px !important;
    }}
    .stAlert p, div[data-testid="stAlert"] p {{ color: {_text2} !important; }}

    /* ── Progress ── */
    .stProgress > div > div > div > div {{
        background: linear-gradient(90deg, {_accent}, {_green2}) !important; border-radius: 8px;
        box-shadow: 0 0 12px rgba(59,130,246,0.3);
    }}
    .stProgress > div > div > div {{ background: {_card_solid} !important; border-radius: 8px; }}

    /* ── Dataframe ── */
    .stDataFrame, div[data-testid="stDataFrame"] {{ background: {_card_solid} !important; border-radius: 12px; }}

    /* ── Buttons ── */
    .stApp button {{
        color: {_text} !important; background: {_card_solid} !important;
        border: 1px solid {_border} !important; border-radius: 10px !important;
        font-weight: 600 !important; transition: all 0.25s cubic-bezier(0.4,0,0.2,1) !important;
        white-space: pre-line !important; line-height: 1.4 !important;
        padding: 0.5rem 1rem !important;
    }}
    .stApp button:hover {{
        background: {_card_solid} !important;
        border-color: {_accent} !important;
        transform: translateY(-1px) !important;
        box-shadow: 0 4px 12px {_shadow} !important;
    }}
    .stApp button p {{ color: inherit !important; white-space: pre-line !important; }}

    /* ── Primary (START) Button - Big & Prominent ── */
    .stApp button[kind="primary"] {{
        background: linear-gradient(135deg, {_green}, #047857) !important;
        color: #fff !important; font-weight: 900 !important; font-size: 16px !important;
        border: none !important; height: 3.5em !important;
        border-radius: 14px !important;
        box-shadow: 0 6px 20px rgba(16,185,129,0.35) !important;
        letter-spacing: 1px !important;
        text-transform: uppercase !important;
    }}
    .stApp button[kind="primary"]:hover {{
        background: linear-gradient(135deg,#047857,#065f46) !important;
        box-shadow: 0 8px 30px rgba(16,185,129,0.45) !important;
        transform: translateY(-2px) !important;
    }}
    .stApp button[kind="primary"] p {{ color: #fff !important; }}

    /* ── Download Button ── */
    div[data-testid="stDownloadButton"] button {{
        background: linear-gradient(135deg,{_accent},#1d4ed8) !important;
        color: #fff !important; font-weight: 700 !important; border: none !important;
        border-radius: 12px !important;
        box-shadow: 0 4px 14px rgba(59,130,246,0.25) !important;
    }}
    div[data-testid="stDownloadButton"] button:hover {{
        background: linear-gradient(135deg,#1d4ed8,#1e40af) !important;
        box-shadow: 0 6px 20px rgba(59,130,246,0.35) !important;
    }}
    div[data-testid="stDownloadButton"] button p {{ color: #fff !important; }}

    /* ── STOP Button ── */
    .stop-btn-wrap button {{
        background: linear-gradient(135deg, {_red}, #b91c1c) !important;
        color: #fff !important; font-weight: 900 !important; font-size: 15px !important;
        border: none !important; height: 3.5em !important;
        border-radius: 14px !important;
        box-shadow: 0 6px 20px rgba(239,68,68,0.3) !important;
        letter-spacing: 1px !important;
        text-transform: uppercase !important;
    }}
    .stop-btn-wrap button:hover {{
        background: linear-gradient(135deg,#b91c1c,#991b1b) !important;
        box-shadow: 0 8px 30px rgba(239,68,68,0.4) !important;
        transform: translateY(-2px) !important;
    }}
    .stop-btn-wrap button p {{ color: #fff !important; }}

    /* ── Status Box ── */
    .sbox {{
        padding: 14px 20px; border-radius: 12px;
        background: {_glass_bg};
        backdrop-filter: blur(10px);
        border: 1px solid {_glass_border};
        text-align: center;
        font-size: 0.92rem; font-weight: 700; color: {_accent2} !important;
    }}

    /* ── Live Log (Terminal Style) ── */
    .llog {{
        background: {'#040810' if is_dark else '#1a1a2e'};
        border: 1px solid {'rgba(59,130,246,0.15)' if is_dark else 'rgba(0,0,0,0.1)'};
        border-radius: 14px;
        padding: 1rem 1.2rem; margin-top: 0.5rem;
        max-height: 220px; overflow-y: auto;
        font-family: 'JetBrains Mono', monospace; font-size: 0.72rem; line-height: 1.8;
        box-shadow: inset 0 2px 8px rgba(0,0,0,0.3);
    }}
    .llog .lt {{
        color: {'#4a5568' if is_dark else '#a0aec0'} !important;
        font-size: 0.58rem; text-transform: uppercase;
        letter-spacing: 3px; margin-bottom: 0.5rem; font-weight: 700;
        padding-bottom: 0.4rem;
        border-bottom: 1px solid {'rgba(255,255,255,0.05)' if is_dark else 'rgba(255,255,255,0.1)'};
    }}
    .llog .ll {{ color: {'#a0aec0' if is_dark else '#cbd5e1'} !important; }}
    .llog .ll .bi {{ color: {_accent2} !important; font-weight: 700; }}
    .llog .ll .sn {{ color: {_green2} !important; }}
    .llog .ll .lok {{ color: {_green2} !important; }}
    .llog .ll .ler {{ color: {_red2} !important; font-weight: 700; }}
    .llog .ll .lpr {{ color: {_yellow2} !important; font-weight: 700; }}
    .llog .ll .lbl {{ color: {_red2} !important; font-weight: 700; }}

    /* ── Banners ── */
    .dbanner {{
        background: {'linear-gradient(135deg,rgba(7,26,18,0.9),rgba(10,38,24,0.9))' if is_dark else 'linear-gradient(135deg,#ecfdf5,#d1fae5)'};
        border: 1px solid {'rgba(22,101,52,0.5)' if is_dark else '#6ee7b7'};
        border-radius: 16px; padding: 2rem; margin: 1rem 0; text-align: center;
        backdrop-filter: blur(10px);
    }}
    .dbanner .dt {{ color: {_green2} !important; font-size: 1.3rem; font-weight: 800; }}
    .dbanner .dm {{ color: {_muted} !important; font-size: 0.8rem; margin-top: 0.5rem;
        font-family: 'JetBrains Mono', monospace; }}

    .sbanner {{
        background: {'linear-gradient(135deg,rgba(26,21,0,0.9),rgba(31,26,0,0.9))' if is_dark else 'linear-gradient(135deg,#fffbeb,#fef3c7)'};
        border: 1px solid {'rgba(133,77,14,0.5)' if is_dark else '#fcd34d'};
        border-radius: 16px; padding: 2rem; margin: 1rem 0; text-align: center;
        backdrop-filter: blur(10px);
    }}
    .sbanner .st2 {{ color: {_yellow2} !important; font-size: 1.3rem; font-weight: 800; }}
    .sbanner .sm {{ color: {_muted} !important; font-size: 0.8rem; margin-top: 0.5rem;
        font-family: 'JetBrains Mono', monospace; }}

    /* ── Misc ── */
    .apbox {{
        background: {'rgba(7,26,18,0.7)' if is_dark else '#ecfdf5'};
        border: 1px solid {'rgba(22,101,52,0.4)' if is_dark else '#6ee7b7'};
        border-radius: 12px; padding: 12px 16px; font-size: 0.8rem; font-weight: 600; color: {_green2} !important;
        backdrop-filter: blur(8px);
    }}
    .apbox .apt {{ font-weight: 800; font-size: 0.85rem; color: {_green2} !important; }}
    .apbox .apd {{ color: {_muted} !important; font-weight: 500; font-size: 0.72rem;
        font-family: 'JetBrains Mono', monospace; }}

    .ramalert {{
        background: {'rgba(26,8,8,0.8)' if is_dark else '#fef2f2'}; border-left: 3px solid {_red};
        border-radius: 0 10px 10px 0; padding: 12px 16px; margin: 0.4rem 0;
        font-size: 0.82rem; font-weight: 700; color: {_red2} !important;
    }}
    .blwarn {{
        color: {_yellow2} !important; font-size: 0.76rem; font-weight: 600;
        padding: 0.4rem 0.7rem; background: {'rgba(26,21,0,0.7)' if is_dark else '#fffbeb'};
        border-left: 3px solid {_yellow}; border-radius: 0 6px 6px 0; margin-top: 0.3rem;
    }}
    .hr2 {{ height: 1px; background: linear-gradient(90deg,transparent,{_border},transparent);
        margin: 1.2rem 0; border: none; }}
    .hrow {{
        background: {_glass_bg}; backdrop-filter: blur(8px);
        border: 1px solid {_glass_border}; border-radius: 12px;
        padding: 0.7rem 1.2rem; margin: 0.4rem 0; font-size: 0.8rem;
        display: flex; justify-content: space-between; align-items: center;
        transition: all 0.2s ease;
    }}
    .hrow:hover {{ border-color: {_accent}; transform: translateX(3px); }}
    .hrow span {{ color: {_text} !important; }}
    .hrow .hd {{ color: {_muted} !important; font-family: 'JetBrains Mono', monospace; font-size: 0.72rem; }}
    .hrow .hr2c {{ color: {_green2} !important; font-weight: 700; }}
    .hrow .hp {{ color: {_accent2} !important; font-weight: 700; }}
    .hrow .ht {{ color: {_purple2} !important; font-family: 'JetBrains Mono', monospace; }}

    .upload-placeholder {{
        background: {_glass_bg}; backdrop-filter: blur(12px);
        border: 2px dashed {_accent}; border-radius: 20px;
        padding: 3.5rem 2rem; text-align: center;
        transition: all 0.3s ease;
        animation: fadeInUp 0.6s ease-out;
    }}
    .upload-placeholder:hover {{ border-color: {_green}; }}
    .upload-placeholder .up-icon {{ font-size: 3rem; margin-bottom: 1rem; }}
    .upload-placeholder .up-title {{ font-size: 1.1rem; font-weight: 800; color: {_text} !important; }}
    .upload-placeholder .up-sub {{ font-size: 0.8rem; color: {_muted} !important; margin-top: 0.4rem; }}

    /* ── Speed Button Selected ── */
    .speed-selected {{
        border: 2px solid {_green} !important;
        box-shadow: 0 0 15px rgba(16,185,129,0.2) !important;
    }}

    /* ── Health Bar ── */
    .health-bar-track {{
        width: 100%; height: 8px; border-radius: 4px;
        background: {_card_solid}; overflow: hidden; margin-top: 6px;
    }}
    .health-bar-fill {{
        height: 100%; border-radius: 4px; transition: width 0.5s ease;
    }}

    /* ── Footer ── */
    .footer {{
        text-align: center; padding: 1.5rem 0; margin-top: 2rem;
        border-top: 1px solid {_border}; font-size: 0.65rem;
        color: {_muted} !important; letter-spacing: 0.5px;
    }}

    /* ── Scrollbar ── */
    ::-webkit-scrollbar {{ width: 5px; }}
    ::-webkit-scrollbar-track {{ background: transparent; }}
    ::-webkit-scrollbar-thumb {{ background: {_border2}; border-radius: 3px; }}
    ::-webkit-scrollbar-thumb:hover {{ background: {_accent}; }}
    </style>
    """, unsafe_allow_html=True)

    return {
        "bg": _bg, "card": _card, "card_solid": _card_solid,
        "border": _border, "border2": _border2,
        "text": _text, "text2": _text2, "muted": _muted, "input": _input,
        "accent": _accent, "accent2": _accent2,
        "green": _green, "green2": _green2, "red": _red, "red2": _red2,
        "yellow": _yellow, "yellow2": _yellow2,
        "purple": _purple, "purple2": _purple2,
        "glass_bg": _glass_bg, "glass_border": _glass_border, "shadow": _shadow,
    }
