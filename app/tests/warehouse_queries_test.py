"""The Lakebase statements of discovery's warehouse checks and reads (queries.warehouse_*): their placeholders match
the parameters main.WarehouseReadStore passes, and each keeps to the rows of its kind."""
import queries as q
import warehouse_reads as wr


def _marks(sql):
    return sql.count("%s")


def test_each_statement_takes_the_parameters_the_store_passes():
    for kind in (wr.CHECK, wr.READ):
        assert _marks(q.warehouse_in_flight_sql(kind)) == 1
        assert _marks(q.warehouse_claim_sql(kind)) == 4
        assert _marks(q.warehouse_progress_sql(kind)) == 1
    assert _marks(q.warehouse_started_sql()) == 3 and _marks(q.warehouse_clear_sql()) == 2
    assert _marks(q.warehouse_check_done_sql()) == 5 and _marks(q.warehouse_read_done_sql()) == 5
    assert _marks(q.warehouse_read_rows_sql()) == 4 and _marks(q.warehouse_left_out_sql()) == 2
    assert _marks(q.warehouse_named_rows_sql(3)) == 12
    assert _marks(q.insert_discovered_tables_sql(wr.TABLE_COLUMNS, 2)) == 2 * len(wr.TABLE_COLUMNS)


def test_a_check_works_on_listed_databases_not_checked_yet_and_a_read_on_running_ones():
    assert "status = 'listed' and (listing -> 'open') is null" in q.warehouse_claim_sql(wr.CHECK)
    assert "status = 'running'" in q.warehouse_claim_sql(wr.READ)
    assert "for update skip locked" in q.warehouse_claim_sql(wr.READ)


def test_a_second_answer_for_a_database_changes_nothing():
    assert q.warehouse_read_done_sql().endswith("and status = 'running'")
    assert q.warehouse_check_done_sql().endswith("and status = 'listed'")
    assert "on conflict (discovery_id, src_database, src_schema, src_table) do nothing" in \
        q.insert_discovered_tables_sql(wr.TABLE_COLUMNS, 1)


def test_the_listing_page_shows_each_databases_check_and_the_totals_count_them():
    assert "(listing ->> 'open')::boolean as open" in q.discovery_listing_sql()
    assert "listing ->> 'why' as why" in q.discovery_listing_sql()
    assert "as closed" in q.discovery_totals_sql() and "as tables" in q.discovery_totals_sql()
