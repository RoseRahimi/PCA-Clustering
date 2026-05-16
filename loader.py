import pandas as pd
import numpy as np
import arcticdb as adb
import nasdaqdatalink as ndl
import time
from tqdm import tqdm
import time
import os

CHUNK_SIZE = 10
RETRY_ATTEMPTS = 3
DELAY_BETWEEN_CHUNKS = 0.5

ndl.ApiConfig.api_key = "9yUyP5fu4aWYHRXYAqxx"
START = "2000-01-01"
END = "2026-02-15"
ARCTIC_URI = "lmdb://./database/arctic"


STOCK_LIBRARY = "stocks_lib"
ETF_LIBRARY = "etfs_lib"
REMOVED_LIBRARY = "removed_stocks"
ac = adb.Arctic(ARCTIC_URI)

def init_arctic():
    """Create the ArcticDB root directory if needed and return the database handle."""
    db_path = ARCTIC_URI.replace("lmdb://", "")
    if not os.path.exists(db_path):
        print(f"Creating ArcticDB at: {db_path}")
        os.makedirs(db_path, exist_ok=True)
    return adb.Arctic(ARCTIC_URI)


_ac = init_arctic()

if STOCK_LIBRARY not in _ac.list_libraries():
    _ac.create_library(STOCK_LIBRARY)

if ETF_LIBRARY not in _ac.list_libraries():
    _ac.create_library(ETF_LIBRARY)

if REMOVED_LIBRARY not in _ac.list_libraries():
    _ac.create_library(REMOVED_LIBRARY)

stock_lib = _ac[STOCK_LIBRARY]
etf_lib = _ac[ETF_LIBRARY]
stock_symbol_cache = set(stock_lib.list_symbols())
etf_symbol_cache = set(etf_lib.list_symbols())

def remember_symbol(cache, symbol):
    """Record a successfully cached symbol in the in-memory lookup set."""
    cache.add(symbol)


def has_symbol(cache, symbol):
    """Check whether a symbol is already known to exist in ArcticDB."""
    return symbol in cache

def fetch_chunked(tickers, table, lib, mapping):
    """Download ticker history in chunks to avoid oversized API requests."""
    chunks = [tickers[i:i + CHUNK_SIZE] for i in range(0, len(tickers), CHUNK_SIZE)]
    existing = set(lib.list_symbols())
    failed_chunks = []

    print(f"Fetching {len(tickers)} tickers from {table}. {len(existing)} already in lib so we are skipping")

    for chunk in tqdm(chunks):
        todo = [t for t in chunk if t not in existing]

        if not todo:
            continue
        df = None
        for attempt in range(RETRY_ATTEMPTS):
            try:
                df = ndl.get_table(
                    table,
                    ticker=",".join(todo),
                    date={"gte": START, "lte": END},
                    paginate=True,
                )

                break
            except Exception as exc:
                if attempt ==RETRY_ATTEMPTS-1:
                    failed_chunks.append((todo, str(exc)))
                else:
                    time.sleep(2 ** attempt)

        if df is None or df.empty:
            continue

        df["date"] = pd.to_datetime(df["date"])

        for tkr, sub in df.groupby("ticker", sort=False):
            sub = (sub.drop(columns="ticker").set_index("date").sort_index())
            lib.write(tkr, sub, metadata={"permaticker": mapping.get(tkr)})
            existing.add(tkr)

        del df
        time.sleep(DELAY_BETWEEN_CHUNKS)
    print(f"done , {sum( len(c) for c, _ in failed_chunks)} failed")
    return failed_chunks


def add_metadata(lib, source_df, fields=None, overwrite=False):
    if fields is None:
        fields = source_df.columns.to_list()

    clean = source_df[fields].where(source_df[fields].notna(), None)
    info_map = clean.to_dict("index")

    updated = missing = unchanged = 0

    for sym in tqdm(lib.list_symbols(), desc="Adding metadata"):
        info = info_map.get(sym)
        if info is None:
            missing += 1
            continue

        existing = lib.read_metadata(sym).metadata or {}
        new_meta = dict (existing)
        for k, v in info.items():
            if overwrite or k not in new_meta:
                new_meta[k] = v

        if new_meta == existing:
            unchanged += 1
            continue

        lib.write_metadata(sym, new_meta)
        updated +=1

    print(f'Updated {updated} symbols, {unchanged} unchanged symbols, {missing} missing symbols')
    return {"updated": updated, "unchanged": unchanged, "missing": missing}


