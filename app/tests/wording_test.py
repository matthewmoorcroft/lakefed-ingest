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


# "Ingestion job" means a schedule now; the bundle-deployed job is "the Databricks job".
DATABRICKS_JOB_AS_INGESTION_JOB = re.compile(r"\bthe ingestion job\b", re.IGNORECASE)


def test_no_page_calls_the_databricks_job_an_ingestion_job():
    offenders = []
    for path in sorted(FRONTEND.rglob("*.tsx")):
        for text in visible_text(path.read_text()):
            if DATABRICKS_JOB_AS_INGESTION_JOB.search(text):
                offenders.append(f"{path.name}: {text.strip()[:80]}")
    new_job = (FRONTEND / "pages" / "NewJob.tsx").read_text()
    if re.search(r"<label>Ingestion job\b", new_job):
        offenders.append("NewJob.tsx: the variant select is labelled Ingestion job")
    assert not offenders, "say 'the Databricks job' for the bundle-deployed job:\n" + "\n".join(offenders)


def test_the_nav_says_schedules_and_jobs_and_its_page_names_the_databricks_jobs():
    app = (FRONTEND / "App.tsx").read_text()
    assert '["/schedules-and-jobs", "Schedules and jobs"]' in app and '["/jobs", "Jobs"]' not in app
    page = (FRONTEND / "pages" / "SchedulesAndJobs.tsx").read_text()
    assert "<h1>Jobs</h1>" not in page and re.search(r"The Databricks jobs the app runs them with", page)


def test_the_backend_names_the_databricks_job():
    main_py = (FRONTEND.parent.parent / "main.py").read_text()
    assert "no ingestion job configured" not in main_py and "no Databricks ingestion job configured" in main_py


def test_discover_says_when_the_login_cant_open_master_and_takes_typed_databases():
    discover = (FRONTEND / "pages" / "Discover.tsx").read_text()
    assert "this connection's login can't open" in discover and "VIEW ANY DATABASE" in discover
    assert "function TypedDatabases" in discover and "Databases typed by hand are read by name" in discover


def test_discover_lists_by_a_name_filter_and_says_which_databases_it_couldnt_read():
    discover = (FRONTEND / "pages" / "Discover.tsx").read_text()
    assert "Databases matching" in discover and "pattern" in discover
    assert "function Unread" in discover and "couldn't be read" in discover
    assert "function databasesOfCsv" in discover and "take them from a CSV file" in discover


def test_discover_can_list_an_azure_sql_server_through_azure_and_reads_the_whole_server_the_same_way():
    discover = (FRONTEND / "pages" / "Discover.tsx").read_text()
    assert "Service credential" in discover and "Server resource id" in discover and "Reader on the server" in discover
    assert "azure_credential" in discover and "azure_resource_id" in discover and "...(tested?.azure ?? {})" in discover


def test_the_target_schema_field_says_a_new_schema_is_made_by_the_first_run():
    form = (FRONTEND / "pages" / "jobForm.tsx").read_text()
    assert "A new schema is made by the first run" in form and "CREATE SCHEMA on the catalog" in form


def test_discover_offers_typed_or_csv_databases_on_the_lab_logins_real_errors():
    import re
    discover = (FRONTEND / "pages" / "Discover.tsx").read_text()
    source = re.search(r"const cantList = .*?/(.+)/i\.test", discover).group(1)
    cant_list = re.compile(source, re.I)
    # lfc_sqlserver_test's login, a user contained in each database, opening master (29 Oct):
    assert cant_list.search("[FAILED_JDBC.CONNECTION] Failed JDBC on the operation: Failed to connect to the database. "
                            "SQLSTATE: HV000")
    assert cant_list.search("Login failed for user 'lfc_ingest'.")
    assert not cant_list.search("the discovery run ended CANCELED")


def test_delete_says_what_goes_and_what_stays_and_offers_the_target_tables():
    page = (FRONTEND / "pages" / "IngestionJob.tsx").read_text()
    assert "Also delete the target tables" in page and "drop_targets: dropTargets" in page
    assert "with their checkpoints. Run history and the change log stay." in page
    assert "another job also writes" in page and "kept_names" in page and "schemas stay" in page
    assert "move to Not in any job" not in page                    # groups always belong to a job: none are left behind
    assert "Run again" in page and "Deleting…" in page             # a pending or failed delete is run again on its own page
    jobs_page = (FRONTEND / "pages" / "IngestionJobs.tsx").read_text()
    assert "Deleted jobs" in jobs_page and "again.mutate" not in jobs_page
