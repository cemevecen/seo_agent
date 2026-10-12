"""GP_SERVICE_ACCOUNT_JSON — .env çift-kaçış parse."""

from backend.services.gp_client import _parse_service_account_json


def test_parse_double_escaped_env_json():
    # Kaynak: .env satırı {\\n  \\"type\\": \\"service_account\\", ...}
    raw = (
        '{\\n  \\"type\\": \\"service_account\\",\\n'
        '  \\"project_id\\": \\"seo-agent-491517\\",\\n'
        '  \\"private_key_id\\": \\"abc\\",\\n'
        '  \\"private_key\\": \\"-----BEGIN PRIVATE KEY-----\\\\nM\\\\n'
        '-----END PRIVATE KEY-----\\\\n\\",\\n'
        '  \\"client_email\\": \\"play-console-api@seo-agent-491517.iam.gserviceaccount.com\\",\\n'
        '  \\"client_id\\": \\"1\\",\\n'
        '  \\"token_uri\\": \\"https://oauth2.googleapis.com/token\\"\\n}'
    )
    info = _parse_service_account_json(raw)
    assert info is not None
    assert info["type"] == "service_account"
    assert info["client_email"].startswith("play-console-api@")
    assert "BEGIN PRIVATE KEY" in info["private_key"]


def test_parse_plain_json():
    raw = (
        '{"type":"service_account","project_id":"p",'
        '"private_key_id":"x","private_key":"-----BEGIN PRIVATE KEY-----\\nA\\n'
        '-----END PRIVATE KEY-----\\n","client_email":"a@b.com",'
        '"client_id":"1","token_uri":"https://oauth2.googleapis.com/token"}'
    )
    info = _parse_service_account_json(raw)
    assert info is not None
    assert info["client_email"] == "a@b.com"
