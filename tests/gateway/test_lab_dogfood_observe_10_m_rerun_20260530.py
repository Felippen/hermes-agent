def test_lab_dogfood_marker_present():
    marker = "lab-dogfood-observe-10-m-rerun-20260530"
    payload = f"gateway smoke marker: {marker}"

    assert marker in payload
