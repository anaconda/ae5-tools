import json

with open(file="system-test-state.json", mode="r", encoding="utf-8") as file:
    FIXTURE_STATE: dict = json.load(file)


def _get_account(id: str) -> dict:
    return [account for account in FIXTURE_STATE["accounts"] if account["id"] == id][0]
