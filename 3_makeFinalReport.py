import json
import os
from glob import glob


def build_observations(account_id):
    period_files = sorted(
            glob(f"periods_{account_id}/account_{account_id}_period_*.json"),
            key=lambda x: int(x.split("_period_")[1].split(".")[0])
    )

    os.makedirs(f"reports/observations_{account_id}", exist_ok=True)

    for i in range(1, len(period_files)):
        with open(period_files[i - 1], "r") as f_prev:
            prev_period = json.load(f_prev)
        with open(period_files[i], "r") as f_curr:
            curr_period = json.load(f_curr)

        cnt_requests = sum(
                sess["requests"]
                for sess in prev_period["phase_one"]
                if sess["handler"].upper() == "CNT"
        )
        ctg_requests = sum(
                sess["requests"]
                for sess in prev_period["phase_two"]
                if sess["handler"].upper() == "CTG"
        )

        total_used = sum(sess["used"] for phase in prev_period.values() for sess in phase)
        total_losses = sum(sess["losses"] for phase in prev_period.values() for sess in phase)

        cnt_percentage = (
            (cnt_requests / (cnt_requests + ctg_requests)) * 100
            if (cnt_requests + ctg_requests) > 0 else 0
        )
        losses_percentage = (
            (total_losses / total_used) * 100 if total_used > 0 else 0
        )

        observation = {
            "observation": {
                "previous_period_summary": {
                    "cnt_percentage": round(cnt_percentage, 2),
                    "cnt_requests": cnt_requests,
                    "losses_percentage": round(losses_percentage, 4),
                    "losses_volume": total_losses
                },
                "current_period": curr_period
            }
        }

        filename = f"reports/observations_{account_id}/account_{account_id}_obs_{i + 1}.json"
        with open(filename, "w") as f_out:
            json.dump(observation, f_out, indent=4)

        print(f"Created {filename}.")


build_observations("500000502_67237c0efdc693ca01192ab2")
