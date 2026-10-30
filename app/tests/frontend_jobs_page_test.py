"""The Ingestion jobs page and the shell, as tidied after a review on the deployed app: tiles that open something
tint as a whole on hover (no underlined text), the job list has its own section header and no colour key, New
ingestion job sits with the filters, and the "Updated" line and Refresh data are at the bottom of the left menu."""
import re
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[1] / "src" / "frontend" / "src"
STYLES = (SOURCE / "styles.css").read_text()
JOBS = (SOURCE / "pages" / "IngestionJobs.tsx").read_text()
APP = (SOURCE / "App.tsx").read_text()


def _rule(selector):
    match = re.search(re.escape(selector) + r"\s*\{([^}]*)\}", STYLES)
    assert match, selector
    return match.group(1)


def test_a_clickable_tile_tints_on_hover_and_never_underlines():
    hover = _rule(".metric-link:hover")
    assert "text-decoration: none" in hover and "background:" in hover
    assert "cursor: pointer" in _rule(".metric-link")
    assert ".metric:hover" not in STYLES                  # a tile that opens nothing has no hover


def test_the_job_list_has_a_header_and_no_colour_key():
    assert '<Section title="Jobs">' in JOBS
    assert "StateKey" not in JOBS


def test_new_ingestion_job_is_on_the_filter_row_at_its_end():
    toolbar = JOBS[JOBS.index('<div className="toolbar">'):JOBS.index('<Section title="Jobs">')]
    assert 'className="primary toolbar-end"' in toolbar and "New ingestion job" in toolbar
    assert "margin-left: auto" in _rule(".toolbar .toolbar-end")
    assert "right=" not in JOBS[JOBS.index("<PageHead"):JOBS.index("/>", JOBS.index("<PageHead"))]


def test_updated_and_refresh_data_are_at_the_bottom_of_the_menu():
    foot = APP[APP.index('<div className="sidebar-foot">'):APP.index("</aside>")]
    assert "<Stamp" in foot and "Refresh data" in foot
    top = APP.index('<div className="topbar">')
    assert "Refresh data" not in APP[top:APP.index("</div>", top)]
    assert "margin-top: auto" in _rule(".sidebar-foot")
    for page in ("IngestionJobs", "Overview", "Sources", "SchedulesAndJobs", "Changes", "Timeline"):
        text = (SOURCE / "pages" / f"{page}.tsx").read_text()
        assert "usePageStamp(" in text and "right={<Stamp" not in text, page
