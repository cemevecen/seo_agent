"""device_names.get_display_name — pazarlama adı eşlemesi."""

from backend.services.device_names import get_display_name


def test_samsung_note_tab_fold_variants():
    assert get_display_name("samsung", "SM-N770F") == "Galaxy Note 10 Lite"
    assert get_display_name("samsung", "SM-N9750") == "Galaxy Note 10+"
    assert get_display_name("samsung", "SM-P610") == "Galaxy Tab S6 Lite"
    assert get_display_name("samsung", "SM-M146B") == "Galaxy M14 5G"
    assert get_display_name("samsung", "SM-F966B") == "Galaxy Z Fold7"


def test_xiaomi_tecno_gm_motorola():
    assert get_display_name("Xiaomi", "M2101K7AG") == "Redmi Note 10 5G"
    assert get_display_name("Xiaomi", "2303CRA44A") == "Redmi 12C"
    assert get_display_name("TECNO", "TECNO KM9") == "Camon 30 5G"
    assert get_display_name("General Mobile", "G318") == "GM 9"
    assert get_display_name("motorola", "moto g85 5G") == "Moto G85 5G"
