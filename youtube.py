#!/usr/bin/env python3
# coding: utf-8
"""
update_links.py  –  מחליף לינק בתיאור כל סרטוני הערוץ
שומר גיבוי CSV   +  ממשיך אוטומטית בין ריצות באמצעות nextPageToken
"""

import csv, json, pickle, time
from pathlib import Path
from typing import Iterator, Tuple

from tqdm import tqdm
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ---------- התאמות שלך ----------
OLD_LINKS = [
    "https://discord.com/invite/fUKMN3q",
    "https://discord.gg/4ZgjkRx",
]
NEW = "https://discord.gg/mrJnesCk2Z"
SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]
MAX_PER_RUN = 200            # None כדי לעבד הכול בריצה אחת
DRY_RUN = False              # True = סימולציה בלבד
SLEEP_SEC = 1                # להישמע למכסת־API
# ---------------------------------

TOKEN_FILE = Path("token.pickle")
STATE_FILE = Path("state.json")
BACKUP_CSV = Path("backup.csv")


# ---------- התחברות ----------
def get_youtube():
    creds = None
    if TOKEN_FILE.exists():
        creds = pickle.loads(TOKEN_FILE.read_bytes())

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "client_secrets.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        TOKEN_FILE.write_bytes(pickle.dumps(creds))

    return build("youtube", "v3", credentials=creds, cache_discovery=False)


def get_uploads_playlist_id(yt):
    resp = yt.channels().list(part="contentDetails", mine=True).execute()
    return resp["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]


# ---------- מצב המשך ----------
def load_start_token() -> str | None:
    if STATE_FILE.exists():
        token = json.loads(STATE_FILE.read_text()).get("nextPageToken")
        return token or None
    return None


def save_next_token(token: str | None):
    if token:
        STATE_FILE.write_text(json.dumps({"nextPageToken": token}, indent=2))
    elif STATE_FILE.exists():
        STATE_FILE.unlink()   # סיימנו – מוחק את הקובץ


# ---------- איטרייטור ----------
def playlist_pages(
    yt, playlist_id: str, start_token: str | None = None
) -> Iterator[Tuple[list[str], str | None]]:
    """מחזיר (videoIds, nextPageToken)"""
    page_token = start_token
    while True:
        resp = (
            yt.playlistItems()
            .list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=page_token,
            )
            .execute()
        )
        vids = [it["contentDetails"]["videoId"] for it in resp["items"]]
        next_token = resp.get("nextPageToken")
        yield vids, next_token
        if not next_token:
            break
        page_token = next_token


# ---------- גיבוי ----------
def append_backup(rows):
    new_file = not BACKUP_CSV.exists()
    with BACKUP_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["videoId", "description"])
        w.writerows(rows)


# ---------- עדכון קישור ----------
def process_batch(
    yt, video_ids: list[str], dry_run: bool
) -> Tuple[int, list[Tuple[str, str]]]:
    updated = 0
    backup = []
    for vid in video_ids:
        snip = yt.videos().list(part="snippet", id=vid).execute()
        if not snip["items"]:
            continue
        snippet = snip["items"][0]["snippet"]
        desc = snippet["description"]
        backup.append((vid, desc))

        if any(old in desc for old in OLD_LINKS):

            for old in OLD_LINKS:
                desc = desc.replace(old, NEW)
            if dry_run:
                print(f"DRY-RUN: would update {vid}")
            else:
                snippet["description"] = desc
                yt.videos().update(
                    part="snippet", body={"id": vid, "snippet": snippet}
                ).execute()
                time.sleep(SLEEP_SEC)
            updated += 1
    return updated, backup


# ---------- פונקציה ראשית ----------
def main():
    yt = get_youtube()
    uploads_id = get_uploads_playlist_id(yt)
    start_token = load_start_token()
    print(f"Start token this run: {start_token}")

    processed = updated_total = 0
    next_token = start_token

    with tqdm(total=MAX_PER_RUN or float("inf"), desc="Scanning") as bar:
        for video_ids, next_token in playlist_pages(yt, uploads_id, start_token):
            # גבול לריצה
            if MAX_PER_RUN and processed >= MAX_PER_RUN:
                break

            upd, backup = process_batch(yt, video_ids, DRY_RUN)
            append_backup(backup)

            processed += len(video_ids)
            updated_total += upd
            bar.update(len(video_ids))

            if MAX_PER_RUN and processed >= MAX_PER_RUN:
                break

    save_next_token(next_token)
    remaining_note = (
        "✔️ אין עוד nextPageToken – כל הפלייליסט טופל."
        if not next_token
        else f"👉 נשמר אסימון המשך: {next_token}"
    )
    print(
        f"\nProcessed {processed} videos, updated {updated_total}."
        f"  Backup: {BACKUP_CSV}\n{remaining_note}"
    )


if __name__ == "__main__":
    main()
