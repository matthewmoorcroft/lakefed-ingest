"""The state bar's colours (styles.css): each state reads as a solid part of one bar."""
import re
from pathlib import Path

STYLES = (Path(__file__).resolve().parent.parent / "src" / "frontend" / "src" / "styles.css").read_text()


def rule(selector: str) -> str:
    """The declarations of the first rule whose selector list contains `selector`."""
    match = re.search(r"(?:^|[},])\s*[^{}]*" + re.escape(selector) + r"\b[^{}]*\{([^}]*)\}", STYLES)
    assert match, selector
    return match.group(1)


def test_waiting_for_a_first_load_is_a_solid_pale_accent_with_no_outline():
    assert "box-shadow" not in rule(".seg-first")
    assert "border" not in rule(".seg-first")
    first = re.search(r"--s-first:\s*([^;]+);", STYLES).group(1)
    assert first.replace(" ", "") == "color-mix(insrgb,var(--db-accent-surface)32%,white)"


COMPONENTS = (Path(__file__).resolve().parent.parent / "src" / "frontend" / "src" / "components.tsx").read_text()


def test_names_wrap_between_words():
    """A space inside a no-wrap word is no break point, so the space between words must sit outside each word."""
    assert "white-space: nowrap" in rule(".word")
    name = re.search(r"export function Name\b.*?\n}\n", COMPONENTS, re.S).group(0)
    word = re.search(r'<span[^>]*className="word"[^>]*>(.*?)</span>', name, re.S).group(1)
    assert '" "' not in word


def test_ids_stay_on_one_line():
    """A cluster id such as 1026-204044-7mghi0m4 never breaks at its hyphens."""
    assert "white-space: nowrap" in rule("td.mono")
