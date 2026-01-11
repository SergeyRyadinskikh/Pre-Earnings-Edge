import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Config:
    fmp_api_key: str
    ib_host: str
    ib_port: int
    ib_client_id: int
    skew_db_path: str
    underlying_db_path: str
    out_dir: str

def load_config() -> Config:
    fmp = (os.getenv("FMP_API_KEY") or "").strip()
    if not fmp:
        raise RuntimeError("Missing FMP_API_KEY in .env")

    skew_path = (os.getenv("SKEW_DB_PATH") or "").strip()
    if not skew_path:
        raise RuntimeError("Missing SKEW_DB_PATH in .env")

    return Config(
        fmp_api_key=fmp,
        ib_host=(os.getenv("IB_HOST") or "127.0.0.1").strip(),
        ib_port=int((os.getenv("IB_PORT") or "4001").strip()),
        ib_client_id=int((os.getenv("IB_CLIENT_ID") or "11").strip()),
        skew_db_path=skew_path,
        underlying_db_path=(os.getenv("UNDERLYING_DB_PATH") or r"data\underlying_daily.sqlite").strip(),
        out_dir=(os.getenv("OUT_DIR") or r"data\out\earnings_edge").strip(),
    )