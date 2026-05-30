def test_lab_dogfood_marker_present():
    marker = "lab-dogfood-observe-10-i-20260530"
    payload = f"lab dogfood marker: {marker}"

    assert marker in payload
