def test_lab_dogfood_10pass_fixed_20260530_i_marker_present():
    marker = "lab_dogfood_10pass_fixed_20260530_i"
    payload = f"gateway smoke marker: {marker}"

    assert marker in payload
