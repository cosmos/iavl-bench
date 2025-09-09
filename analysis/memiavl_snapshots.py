from datetime import datetime


def capture_memiavl_snapshot_log(row, snapshots):
    msg = row.get("msg")
    time = datetime.fromisoformat(row.get("time"))
    version = row.get("version")
    match msg:
        case "start rewriting snapshot":
            snapshots += [{"version": version, "start": time}]
        case "finished rewriting snapshot":
            snapshots[-1]["end"] = time
        case "finished best-effort WAL catchup":
            snapshots[-1]["wal_catchup"] = time
        case "switched to new snapshot":
            snapshots[-1]["switch_time"] = time
            snapshots[-1]["switch_version"] = version
