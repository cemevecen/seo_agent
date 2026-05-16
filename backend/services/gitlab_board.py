import os
import httpx
from typing import Any

GITLAB_URL = "https://git.nokta.com/api/v4"

def get_gitlab_token() -> str:
    return os.environ.get("GITLAB_PRIVATE_TOKEN") or ""

def fetch_project_board(project_path: str) -> dict[str, Any]:
    """GitLab projesinin ilk board'unu ve issue'larını çeker."""
    token = get_gitlab_token()
    if not token:
        return {"error": "Token bulunamadı"}

    headers = {"PRIVATE-TOKEN": token}
    encoded_path = project_path.replace("/", "%2F")
    
    # 1. Proje ID'sini ve Board ayarlarını al
    boards_url = f"{GITLAB_URL}/projects/{encoded_path}/boards"
    try:
        resp = httpx.get(boards_url, headers=headers, timeout=10.0)
        if resp.status_code != 200:
            return {"error": f"Board alınamadı: {resp.status_code}"}
        boards = resp.json()
        if not boards:
            return {"error": "Projede aktif board bulunamadı"}
        
        main_board = boards[0]
        
        # 2. Açık Issue'ları al (Board listelerine göre gruplayacağız)
        issues_url = f"{GITLAB_URL}/projects/{encoded_path}/issues?state=opened&per_page=100"
        resp_issues = httpx.get(issues_url, headers=headers, timeout=10.0)
        issues = resp_issues.json() if resp_issues.status_code == 200 else []
        
        return {
            "board": main_board,
            "issues": issues,
            "project_path": project_path
        }
    except Exception as e:
        return {"error": str(e)}

def move_issue(project_path: str, issue_iid: int, add_labels: list[str], remove_labels: list[str]) -> bool:
    """Issue'nun etiketlerini güncelleyerek board üzerinde sütun değiştirir."""
    token = get_gitlab_token()
    if not token:
        return False
        
    headers = {"PRIVATE-TOKEN": token}
    encoded_path = project_path.replace("/", "%2F")
    url = f"{GITLAB_URL}/projects/{encoded_path}/issues/{issue_iid}"
    
    data = {}
    if add_labels:
        data["add_labels"] = ",".join(add_labels)
    if remove_labels:
        data["remove_labels"] = ",".join(remove_labels)
        
    resp = httpx.put(url, headers=headers, json=data)
    return resp.status_code == 200
