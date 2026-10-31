"""
Direct GitHub Uploader using GitHub REST API.
Uploads all repository files directly to GitHub without requiring Git CLI or libcurl.
Usage:
    python scripts/push_to_github.py --token <GITHUB_PERSONAL_ACCESS_TOKEN>
"""

import argparse
import base64
import json
from pathlib import Path
import sys
import urllib.request
import urllib.error

REPO_OWNER = "Malleswarikomara"
REPO_NAME = "CDC_DataPipeline-Project"
BASE_DIR = Path(__file__).resolve().parent.parent

# Files and directories to ignore
IGNORE_PATHS = {
    ".git",
    ".pytest_cache",
    "__pycache__",
    ".vscode",
    "data/large/cdc_events_100000.jsonl",
    "data/demo_cdc_pipeline.db",
    "data/cdc_pipeline.db",
    "placeholder.txt",
}


def should_ignore(rel_path: Path) -> bool:
    rel_str = str(rel_path).replace("\\", "/")
    for ig in IGNORE_PATHS:
        if rel_str == ig or rel_str.startswith(ig + "/"):
            return True
    if rel_str.endswith(".pyc") or rel_str.endswith(".db"):
        return True
    return False


def get_all_files(base_dir: Path):
    files = []
    for p in base_dir.rglob("*"):
        if p.is_file():
            rel = p.relative_to(base_dir)
            if not should_ignore(rel):
                files.append((p, str(rel).replace("\\", "/")))
    return files


def upload_file_to_github(file_path: Path, repo_path: str, token: str):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{repo_path}"
    
    with open(file_path, "rb") as f:
        content_bytes = f.read()
    
    content_b64 = base64.b64encode(content_bytes).decode("utf-8")
    
    # Check if file already exists to get SHA
    sha = None
    req_check = urllib.request.Request(url, headers={
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "CDC-Pipeline-Uploader",
    })
    try:
        with urllib.request.urlopen(req_check) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            sha = data.get("sha")
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise

    payload = {
        "message": f"Upload {repo_path}",
        "content": content_b64,
        "branch": "main",
    }
    if sha:
        payload["sha"] = sha

    req_put = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
            "Content-Type": "application/json",
            "User-Agent": "CDC-Pipeline-Uploader",
        },
        method="PUT"
    )

    with urllib.request.urlopen(req_put) as resp:
        return resp.status in (200, 201)


def main():
    parser = argparse.ArgumentParser(description="Upload project to GitHub via REST API")
    parser.add_argument("--token", "-t", type=str, required=True, help="GitHub Personal Access Token (classic with repo scope)")
    args = parser.parse_args()

    files = get_all_files(BASE_DIR)
    print(f"Found {len(files)} files to upload to {REPO_OWNER}/{REPO_NAME} on branch 'main'...")

    success_cnt = 0
    for local_path, repo_path in files:
        try:
            print(f"Uploading: {repo_path} ...", end=" ", flush=True)
            upload_file_to_github(local_path, repo_path, args.token)
            print("OK")
            success_cnt += 1
        except Exception as e:
            print(f"FAILED: {e}")

    print(f"\nCompleted! Successfully uploaded {success_cnt}/{len(files)} files.")
    print(f"Repository URL: https://github.com/{REPO_OWNER}/{REPO_NAME}")


if __name__ == "__main__":
    main()
