import json
from collections import defaultdict
from datetime import datetime

with open("sample_dataset.json") as file:
    data = json.load(file)

grouped = defaultdict(list)
for item in data:
    if item.get("requestType") == "TICKET":
        grouped[item["protocolSessionId"]].append(item)
    else:
        grouped[item["protocolSessionId"]].append(item)

results = []

for session_id, items in grouped.items():
    if len(items) == 1 and items[0].get("requestType") == "TICKET":
        if items[0]["usedQuota"] == 0 and items[0]["usedQuota"] == 0 and items[0]["sessionDurationSeconds"] is None:
            continue

        ticket = {
            "protocolSessionId": session_id,
            "account": items[0]["serviceReferenceId"],
            "duration": items[0]["sessionDurationSeconds"],
            "date": items[0]["userEventTimestamp"],
            "used": items[0]["usedQuota"],
            "losses": items[0]["lossesAmnt"],
            "requests": items[0]["sessionRequests"],
            "handler": "CTG"
        }

        results.append(ticket)
        continue

    items.sort(key=lambda x: datetime.fromisoformat(x["userEventTimestamp"]))

    first_time = datetime.fromisoformat(items[0]["userEventTimestamp"])
    last_time = datetime.fromisoformat(items[-1]["userEventTimestamp"])
    duration = (last_time - first_time).total_seconds()

    total_used_quota = sum(i.get("usedQuota", 0) for i in items)
    total_losses = sum(i.get("lossesAmnt", 0) for i in items)
    total_requests = sum(i.get("sessionRequests", 0) for i in items)

    aggregated = {
        "protocolSessionId": session_id,
        "account": items[0]["serviceReferenceId"],
        "date": items[0]["userEventTimestamp"],
        "duration": duration,
        "used": total_used_quota,
        "losses": total_losses,
        "requests": total_requests,
        "handler": "CNT"
    }

    results.append(aggregated)

with open("grouped_data.json", "w") as outfile:
    json.dump(results, outfile, indent=4)

print("Aggregation complete. Results saved to grouped_data.json")

with open("grouped_data.json", "r") as file:
    grouped_data = json.load(file)

accounts = defaultdict(list)
for item in grouped_data:
    account_id = item["account"]
    accounts[account_id].append(item)

for account_id, items in accounts.items():
    filename = f"account_{account_id}.json"
    with open(filename, "w") as outfile:
        json.dump(items, outfile, indent=4)
    print(f"Saved {len(items)} sessions to {filename}.")

print("All accounts have been split into separate files.")
