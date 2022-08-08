import requests
from sqlmodel import SQLModel

def extract(url: str, model: SQLModel) -> list[SQLModel]:
    response = requests.get(url=url)
    result = response.json()
    records = result.get('records', [])
    rows = (model(**r) for r in records)
    return list(rows)