"""Settings tab — appearance, system status (Selenium / Chrome), about block."""
import streamlit as st

from scraper.driver import _CHROME_BIN, _CHROME_DEBUG, _CHROME_DRV


def render(colors, is_dark, selenium_ok):
    _green2 = colors["green2"]
    _red2 = colors["red2"]
    _glass_bg = colors["glass_bg"]
    _glass_border = colors["glass_border"]
    _muted = colors["muted"]

    st.markdown('<div class="sec">Appearance</div>', unsafe_allow_html=True)
    st.markdown(
        f'''<div class="stat-cell" style="margin:0.5rem 0">
        <span class="sl">Current Theme</span>
        <span class="sv">{"Dark Mode" if is_dark else "Light Mode"}</span>
    </div>''',
        unsafe_allow_html=True,
    )
    st.caption("Use the button in the top-right corner to switch between dark and light themes.")

    st.markdown('<div class="sec">System Status</div>', unsafe_allow_html=True)
    st.markdown(
        f'''
    <div class="stat-grid">
        <div class="stat-cell"><span class="sl">Selenium</span><span class="sv" style="color:{_green2 if selenium_ok else _red2}!important">{"Ready" if selenium_ok else "Not Found"}</span></div>
        <div class="stat-cell"><span class="sl">Chrome Binary</span><span class="sv" style="color:{_green2 if _CHROME_BIN else _red2}!important">{_CHROME_BIN or "Not Found"}</span></div>
        <div class="stat-cell"><span class="sl">ChromeDriver</span><span class="sv" style="color:{_green2 if _CHROME_DRV else _red2}!important">{_CHROME_DRV or "Not Found"}</span></div>
        <div class="stat-cell"><span class="sl">Installed Packages</span><span class="sv" style="font-size:0.65rem">{_CHROME_DEBUG}</span></div>
        <div class="stat-cell"><span class="sl">Hosting</span><span class="sv">Streamlit Cloud</span></div>
    </div>
    ''',
        unsafe_allow_html=True,
    )

    st.markdown('<div class="sec">About</div>', unsafe_allow_html=True)
    st.markdown(
        f'''<div style="text-align:center; padding:2rem; background:{_glass_bg}; backdrop-filter:blur(12px);
        border:1px solid {_glass_border}; border-radius:16px;">
        <div style="font-size:1.6rem; font-weight:900; margin-bottom:0.5rem;">3LINES DataHunter</div>
        <div style="font-size:0.82rem; color:{_muted}!important; line-height:1.8;">
            v16.0 Elite Edition<br>
            Smart Filtering &bull; Auto-Retry &bull; Multi-Format Export<br>
            Priority Targets &bull; Blacklist Exclusion &bull; Auto-Save
        </div>
    </div>''',
        unsafe_allow_html=True,
    )
