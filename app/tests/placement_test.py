"""How a job's databases are placed in its groups (placement.py): as many groups as the tables need with no group for
a small remainder, spread evenly by tables, never splitting a database, and later databases into the lightest group."""
import placement


def test_the_default_number_of_groups_leaves_out_a_small_remainder():
    assert placement.groups_for_tables(100_200) == 8
    assert placement.groups_for_tables(12_500 * 8 + 1_251) == 9
    assert [placement.groups_for_tables(n) for n in (0, 1, 12_500, 13_750, 13_751)] == [1, 1, 1, 1, 2]


def test_databases_are_spread_biggest_first_into_the_lightest_group():
    spread = placement.even([("a", 50), ("b", 40), ("c", 30), ("d", 20), ("e", 10)], 2)
    loads = [sum(t for name, t in (("a", 50), ("b", 40), ("c", 30), ("d", 20), ("e", 10)) if spread[name] == g)
             for g in (0, 1)]
    assert sorted(loads) == [70, 80]


def test_the_same_databases_always_land_the_same_way_whatever_their_order_or_case():
    rows = [("Eng_%03d" % i, (i * 37) % 11 + 1) for i in range(50)]
    assert placement.even(rows, 4) == placement.even(list(reversed([(n.lower(), t) for n, t in rows])), 4)


def test_a_database_listed_twice_is_one_database_in_one_group():
    spread = placement.even([("a", 3), ("A", 4), ("b", 5)], 2)
    assert set(spread) == {"a", "b"} and spread["a"] != spread["b"]


def test_later_databases_join_the_lightest_group_and_nothing_moves():
    join = placement.Lightest({"sql_a": {"g1": 10, "g2": 4}})
    assert join.place("sql_a", "new_1", 5) == "g2"            # 4 + 5 = 9
    assert join.place("sql_a", "new_2", 5) == "g2"            # 9 is still lighter than 10
    assert join.place("sql_a", "new_3", 1) == "g1"            # 10 against 14
    assert join.place("sql_a", "NEW_1") == "g2"               # placed already: it keeps its group
    assert join.place("sql_b", "x") is None and not join.knows("sql_b")
