import os
import httpx
import asyncio
from typing import Any

GITLAB_URL = "https://git.nokta.com/api/v4"

def get_gitlab_token() -> str:
    return os.environ.get("GITLAB_PRIVATE_TOKEN") or ""

async def fetch_project_board_async(project_path: str) -> dict[str, Any]:
    """GitLab projesinin ilk board'unu ve issue'larını eşzamanlı (async) çeker."""
    token = get_gitlab_token()
    if not token:
        return {"error": "Token bulunamadı. Lütfen .env dosyanıza GITLAB_PRIVATE_TOKEN ekleyin."}

    headers = {"PRIVATE-TOKEN": token}
    encoded_path = project_path.replace("/", "%2F")
    boards_url = f"{GITLAB_URL}/projects/{encoded_path}/boards"
    issues_url = f"{GITLAB_URL}/projects/{encoded_path}/issues?state=opened&per_page=100"

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            # İki isteği aynı anda atıyoruz (Süper hızlı olması için)
            boards_task = client.get(boards_url, headers=headers)
            issues_task = client.get(issues_url, headers=headers)
            
            resp_boards, resp_issues = await asyncio.gather(boards_task, issues_task)
            
            if resp_boards.status_code != 200:
                return {"error": f"Board alınamadı: {resp_boards.status_code} - {resp_boards.text}"}
                
            boards = resp_boards.json()
            if not boards:
                return {"error": "Projede aktif board bulunamadı"}
                
            main_board = boards[0]
            issues = resp_issues.json() if resp_issues.status_code == 200 else []
            
            return {
                "board": main_board,
                "issues": issues,
                "project_path": project_path
            }
        except Exception as e:
            return {"error": str(e)}

async def move_issue_async(project_path: str, issue_iid: int, add_labels: list[str], remove_labels: list[str]) -> bool:
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
        
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.put(url, headers=headers, json=data)
            return resp.status_code == 200
        except Exception:
            return False
