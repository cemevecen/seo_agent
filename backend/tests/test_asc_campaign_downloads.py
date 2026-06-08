from backend.services.asc_campaign_downloads import campaign_name_matches, DOVIZ_BANNER_CAMPAIGN_PATTERNS


def test_campaign_name_matches():
    assert campaign_name_matches("mdoviz_app_download_banner", DOVIZ_BANNER_CAMPAIGN_PATTERNS)
    assert campaign_name_matches("mdoviz%20app%20download%20banner", DOVIZ_BANNER_CAMPAIGN_PATTERNS)
    assert not campaign_name_matches("mweb", DOVIZ_BANNER_CAMPAIGN_PATTERNS)
