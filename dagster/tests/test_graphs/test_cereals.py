from src.jobs.cereals import cereal_insights


def test_cereal_insights():
    res = cereal_insights.execute_in_process()
    assert res.success
    assert res.output_for_node("find_highest_protein_cereal") == "Special K"
