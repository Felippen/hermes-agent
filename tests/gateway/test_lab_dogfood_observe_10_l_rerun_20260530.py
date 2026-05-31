def test_lab_dogfood_marker_is_present():
    marker = "lab dogfood observe 10 l rerun 20260530"

    assert "lab dogfood" in marker
