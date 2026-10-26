"""The app's words: what the control table calls a task_collection, the app shows as an "ingestion group".

The column, the job parameters and the API keep their names; only text a person reads changes. This scans the
frontend's visible text (JSX text, and the strings of labels, titles, notes, column headers and confirmations) and
the backend's messages for the old word."""
import re
from pathlib import Path

import schedules as sch

FRONTEND = Path(__file__).resolve().parent.parent / "src" / "frontend" / "src"
OLD_WORD = re.compile(r"\bcollections?\b", re.IGNORECASE)
COMMENTS = re.compile(r"/\*.*?\*/|//[^\n]*", re.DOTALL)
JSX_TEXT = re.compile(r">([^<>{}]+)<")
VISIBLE_STRINGS = re.compile(
    r"\b(?:label|title|note|headerName|confirmLabel|aria-label|placeholder|crumb)\s*[=:]\s*[{(]?\s*[\"`]([^\"`]*)[\"`]")
STRING_LITERALS = re.compile(r"\"([^\"\n]*)\"|`([^`\n]*)`")


def visible_text(source: str) -> list[str]:
    code = COMMENTS.sub("", source)
    # Text between tags, but not TypeScript between generics' angle brackets (it carries code punctuation).
    found = [m.group(1) for m in JSX_TEXT.finditer(code) if not re.search(r"[;=()\[\]]", m.group(1))]
    found += [m.group(1) for m in VISIBLE_STRINGS.finditer(code)]
    # Any other literal with a space in it reads as a phrase (message, notice); paths and keys have none.
    found += [s for m in STRING_LITERALS.finditer(code) for s in m.groups() if s and " " in s and not s.startswith("/")]
    return found


def test_no_page_shows_the_word_collection():
    offenders = []
    for path in sorted(FRONTEND.rglob("*.tsx")):
        for text in visible_text(path.read_text()):
            cleaned = re.sub(r"\$\{[^}]*\}|\btask_collection\b|\bTAG_COLLECTION\b", "", text)
            if OLD_WORD.search(cleaned):
                offenders.append(f"{path.name}: {text.strip()[:80]}")
    assert not offenders, "use 'ingestion group' in the app's wording:\n" + "\n".join(offenders)


def test_the_check_catches_the_old_word():
    sample = '<th>Collection</th> <Section title="Collections" /> setMessage(`Refused while the collection runs`)'
    assert len([t for t in visible_text(sample) if OLD_WORD.search(t)]) == 3


def test_schedule_messages_say_ingestion_group():
    errors = " ".join(sch.validate(dict(name="s", cadence_minutes=60, timezone="UTC", members=["a", "a", "zz"]),
                                   ["a"], ["default"]))
    assert "ingestion group" in errors and not OLD_WORD.search(errors.replace("members:", ""))
