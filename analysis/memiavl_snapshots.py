from datetime import datetime


def capture_memiavl_snapshot_log(row, snapshots):
    msg = row.get("msg")
    time = datetime.fromisoformat(row.get("time"))
    version = row.get("version")
    match msg:
        case "start rewriting snapshot":
            snapshots += [{"version": version, "start_time": time}]
        case "finished rewriting snapshot":
            snapshots[-1]["end_time"] = time
            snapshots[-1]["snapshot_duration"] = time - snapshots[-1]["start_time"]
        case "finished best-effort WAL catchup":
            snapshots[-1]["best_effort_wal_time"] = time
            snapshots[-1]["best_effort_wal_duration"] = time - snapshots[-1]["end_time"]
        case "switched to new snapshot":
            snapshots[-1]["switch_time"] = time
            snapshots[-1]["wal_sync_duration"] = time - snapshots[-1]["best_effort_wal_time"]
            snapshots[-1]["switch_version"] = version
