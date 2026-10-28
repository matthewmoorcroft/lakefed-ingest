"""The ingestion job's per-app-job copies (cluster_copies): what a copy is, and when one is made, kept or deleted."""
import cluster_copies as cc

BUNDLE = {
    "name": "[dev someone] lakefed_ingest", "max_concurrent_runs": 100,
    "schedule": {"quartz_cron_expression": "0 0/15 * * * ?", "timezone_id": "UTC", "pause_status": "PAUSED"},
    "deployment": {"kind": "BUNDLE", "metadata_file_path": "/x/metadata.json"}, "edit_mode": "UI_LOCKED",
    "job_clusters": [{"job_cluster_key": "sweep_cluster", "new_cluster": {
        "spark_version": "18.x-scala2.13", "node_type_id": "Standard_D8ds_v5", "driver_node_type_id": "Standard_D16ds_v5",
        "num_workers": 1, "spark_conf": {"spark.scheduler.mode": "FAIR"}}}],
    "tasks": [{"task_key": "ingest", "job_cluster_key": "sweep_cluster", "notebook_task": {"notebook_path": "/x/ingest"}}],
    "parameters": [{"name": "task_collection", "default": ""}], "tags": {"project": "lakefed_ingest"},
}
BIG = {"driver_node_type_id": "Standard_D32ds_v5", "node_type_id": "Standard_D16ds_v5", "num_workers": 4}


def schedule(sid, name="Hourly", cluster=None, deleted=False):
    return {"id": sid, "name": name, "options": {"cluster": cluster} if cluster else {},
            "deleted_at": "2026-10-28" if deleted else None}


def test_a_copy_is_the_bundle_job_with_only_its_cluster_changed_and_no_trigger_of_its_own():
    s = cc.copy_settings(BUNDLE, schedule(7, cluster=BIG), BIG, "inst/db")
    cluster = s["job_clusters"][0]["new_cluster"]
    assert (cluster["driver_node_type_id"], cluster["node_type_id"], cluster["num_workers"]) == (
        "Standard_D32ds_v5", "Standard_D16ds_v5", 4)
    assert cluster["spark_conf"] == {"spark.scheduler.mode": "FAIR"} and s["tasks"] == BUNDLE["tasks"]
    assert s["parameters"] == BUNDLE["parameters"] and s["max_concurrent_runs"] == 100
    assert not {"schedule", "deployment", "edit_mode"} & set(s)
    assert s["name"] == "lakefed_ingest: Hourly"
    assert s["tags"] == {"project": "lakefed_ingest", cc.TAG_SCHEDULE: "7", cc.TAG_OWNER: "inst/db"}
    assert BUNDLE["job_clusters"][0]["new_cluster"]["num_workers"] == 1      # the bundle's settings are untouched


def test_a_worker_count_replaces_an_autoscale_range():
    bundle = {**BUNDLE, "job_clusters": [{"job_cluster_key": "c", "new_cluster": {"autoscale": {"min_workers": 1, "max_workers": 8}}}]}
    cluster = cc.copy_settings(bundle, schedule(1), {"num_workers": "2"}, "o")["job_clusters"][0]["new_cluster"]
    assert cluster == {"num_workers": 2}


def test_jobs_with_their_own_cluster_get_a_copy_and_jobs_back_at_the_defaults_lose_theirs():
    schedules = [schedule(1, "New", BIG), schedule(2, "Kept", BIG), schedule(3, "Back to defaults"),
                 schedule(4, "Deleted", BIG, deleted=True)]
    actions = cc.plan(schedules, {"2": 902, "3": 903, "4": 904, "99": 999}, BUNDLE, "o")
    assert [(a["action"], a["schedule_id"], a.get("job_id")) for a in actions] == [
        ("create", 1, None), ("update", 2, 902), ("delete", 3, 903), ("delete", 4, 904)]
    # A copy of a schedule this run wasn't asked about (99) is left alone.


def test_blank_cluster_values_mean_the_bundle_jobs_own():
    assert cc.cluster_of(schedule(1, cluster={"driver_node_type_id": "", "num_workers": 2})) == {"num_workers": 2}
    assert cc.cluster_of(schedule(1)) is None


def test_a_copy_is_shared_as_the_ingestion_job_is_with_the_app_managing_it():
    acl = [{"user_name": "deployer@x.com", "all_permissions": [{"permission_level": "IS_OWNER"}]},
           {"group_name": "admins", "all_permissions": [{"permission_level": "CAN_MANAGE", "inherited": True}]},
           {"group_name": "operators", "all_permissions": [{"permission_level": "CAN_MANAGE_RUN"}]},
           {"service_principal_name": "app-sp", "all_permissions": [{"permission_level": "CAN_MANAGE"}]}]
    assert cc.copy_permissions(acl, "app-sp", "deployer@x.com") == [
        {"user_name": "deployer@x.com", "permission_level": "IS_OWNER"},
        {"group_name": "operators", "permission_level": "CAN_MANAGE_RUN"},
        {"service_principal_name": "app-sp", "permission_level": "CAN_MANAGE"}]
