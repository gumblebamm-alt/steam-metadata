# Steam Metadata & Depot Manifest Historical Tracker



An automated tool to scan, store, and track historical version changes (`BuildID`, `Branches`, `Depot Manifest GID`) for all Steam games (approximately 184k games).

---

## Core Features



* **Append-Only Storage (Permanent History)**: When a new update is detected (new `BuildID` or changed Depot Manifest GID), the tool appends a record to the `history` list instead of overwriting it. This preserves all old game patches, allowing future Launchers or Depot Downloaders to query and download previous versions on demand.


* **Modulo Sharding (`appid % 1000`)**: Data is evenly distributed across 1,000 subdirectories (`data/000/` to `data/999/`). Each folder contains an average of 180 `.json` files, completely preventing Windows file explorer freezes and Git/GitHub/Hugging Face index overloads. This enables $O(1)$ direct file lookups for Launchers:


```javascript
const shard = String(appId % 1000).padStart(3, '0');
const url = `https://raw.githubusercontent.com/YOUR_REPO/main/data/${shard}/${appId}.json`;

```


* **Anti-Rate Limit & Safe Checkpoint/Resume**: Automatically catches HTTP 429 errors using Exponential Backoff with Jitter to avoid IP bans. Scanning progress is continuously saved to `cache/checkpoint.json`. Users can press `Ctrl+C` to safely stop at any time and resume later without duplicate scanning.



---

## Directory Structure



```text
tools/steam_manifest_tracker/
├── data/                    # Sharded data (appid % 1000)
│   ├── 360/                 # Shard 360
│   │   └── 945360.json      # Among Us metadata file
│   └── ...
├── cache/
│   ├── applist.json         # List of all Steam AppIDs (~165k-184k)
│   └── checkpoint.json      # Checkpoint state for resuming
├── tracker.py               # Main script
├── fetcher.py               # Async network fetcher with backoff & semaphore
├── storage.py               # Shard storage & append-only management
├── applist_provider.py      # AppID list downloader & cache
├── run_tracker.bat          # 1-click interactive runner
└── README.md

```

---

## Usage Guide



**Method 1: 1-Click Menu**


Double-click the `run_tracker.bat` file to access the following options:

* `1`: Test scan popular games (Among Us, CS2, Terraria, Dota 2, RE Requiem).


* `2`: Full Steam library scan (automatically resumes from your stopped position).


* `3`: Test scan the first 50 games.


* `4`: Refresh the AppList from the server.


* `5`: View statistics on saved files and shards.



**Method 2: Command Line**

* **Test scan specific games**: `python tracker.py --appids 945360,730,105600`

* **Scan 100 sample games**: `python tracker.py --limit 100 --concurrency 5`

* **Full library background scan**: `python tracker.py --run --concurrency 5`

* **View current data statistics**: `python tracker.py --stats`

* **Refresh AppList with your Steam Web API Key (if available)**: `python tracker.py --fetch-applist --steam-key YOUR_STEAM_API_KEY`


---

## Standard JSON Structure (`{appid}.json`)



The tool uses a **Hybrid (100% Full Raw + `_history`)** storage model:

```json
{
  "_lastChecked": 1788614553,
  "_history": [
    {
      "buildId": "24302054",
      "branch": "public",
      "timeUpdated": 1787072415,
      "firstSeen": 1788614553,
      "manifests": {
        "945361": {
          "gid": "1397756378225229500",
          "size": 1115879348,
          "download": 656543488
        }
      }
    }
  ],
  "appid": "945360",
  "common": {
    "name": "Among Us",
    "type": "Game",
    "icon": "b82c3f46da...",
    "clienticon": "4096637...",
    "oslist": "windows,macos",
    "languages": { ... }
  },
  "config": {
    "installdir": "Among Us",
    "launch": {
      "0": {
        "executable": "Among Us.exe",
        "config": { "oslist": "windows" }
      }
    }
  },
  "depots": { ... },
  "extended": { ... },
  "ufs": { ... }
}

```

* **`_history`**: Permanently preserves the complete history of older versions when a game updates (Append-Only).


* **Raw Data (`common`, `config`, `depots`, `ufs`, `extended`)**: Retains 100% of the original Steam data so Launchers can properly read `.exe` file names, install directories, Cloud Saves, and high-quality icons/logos.
