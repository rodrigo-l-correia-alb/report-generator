import json
import os
from datetime import datetime


def split_into_periods(account_file):
    with open(account_file, "r") as f:
        sessions = json.load(f)

    sessions.sort(key=lambda x: datetime.fromisoformat(x["date"]))

    phases = []
    current_phase = []
    current_handler = None

    for session in sessions:
        handler = session["handler"]

        if current_handler is None or handler == current_handler:
            current_phase.append(session)
        else:
            phases.append({
                "handler": current_handler,
                "sessions": current_phase
            })
            current_phase = [session]

        current_handler = handler

    if current_phase:
        phases.append({
            "handler": current_handler,
            "sessions": current_phase
        })

    periods = []
    for i in range(0, len(phases) - 1, 2):
        phase_one = phases[i]["sessions"]
        phase_two = phases[i + 1]["sessions"]

        periods.append({
            "phase_one": phase_one,
            "phase_two": phase_two
        })

    account_id = sessions[0]["account"]
    os.makedirs(f"periods_{account_id}", exist_ok=True)

    for idx, period in enumerate(periods, start=1):
        filename = f"periods_{account_id}/account_{account_id}_period_{idx}.json"
        with open(filename, "w") as f:
            json.dump(period, f, indent=4)
        print(f"Saved {filename}.")


if __name__ == "__main__":
    for fname in os.listdir("."):
        if fname.startswith("account") and fname.endswith(".json"):
            try:
                print(f"Processing {fname}...")
                split_into_periods(fname)
            except Exception as e:
                print(f"Error processing {fname}: {e}")
