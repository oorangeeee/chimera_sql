#!/usr/bin/env python3
"""验证种子 SQL 结果文件中 Oracle 与 SQLite 的执行结果是否一致。

支持两种模式：
- strict: 逐元素严格比较
- normalized: 按项目约定进行结果归一化后比较
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple


DEFAULT_EPS = 1e-5


def _is_bool_like(value: Any) -> bool:
    if isinstance(value, bool):
        return True
    if isinstance(value, int) and value in (0, 1):
        return True
    return False


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return bool(value)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _values_equal(a: Any, b: Any, mode: str, eps: float) -> bool:
    if mode == "strict":
        return a == b

    # normalized mode
    if (a is None or a == "") and (b is None or b == ""):
        return True

    if _is_bool_like(a) and _is_bool_like(b):
        return _to_bool(a) == _to_bool(b)

    if _is_number(a) and _is_number(b):
        return abs(float(a) - float(b)) <= eps

    return a == b


def _compare_rows(
    rows_a: List[List[Any]],
    rows_b: List[List[Any]],
    mode: str,
    eps: float,
) -> Tuple[bool, str]:
    if len(rows_a) != len(rows_b):
        return False, f"row_count mismatch: {len(rows_a)} vs {len(rows_b)}"

    for row_idx, (row_a, row_b) in enumerate(zip(rows_a, rows_b), start=1):
        if len(row_a) != len(row_b):
            return False, f"row {row_idx} col_count mismatch: {len(row_a)} vs {len(row_b)}"
        for col_idx, (a, b) in enumerate(zip(row_a, row_b), start=1):
            if not _values_equal(a, b, mode, eps):
                return (
                    False,
                    f"row {row_idx} col {col_idx} mismatch: {a!r} vs {b!r}",
                )
    return True, ""


def _load_results(path: Path) -> Dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if "results" not in data:
        raise ValueError("invalid results file: missing 'results'")
    return data


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify Oracle vs SQLite results from seed SQL run.",
    )
    parser.add_argument("result_file", help="path to seed_sql_results_*.json")
    parser.add_argument(
        "--mode",
        choices=("normalized", "strict"),
        default="normalized",
        help="comparison mode (default: normalized)",
    )
    parser.add_argument(
        "--eps",
        type=float,
        default=DEFAULT_EPS,
        help="epsilon for numeric comparison in normalized mode (default: 1e-5)",
    )
    parser.add_argument(
        "--max-diffs",
        type=int,
        default=20,
        help="max diff entries to print (default: 20)",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    path = Path(args.result_file)
    payload = _load_results(path)
    results = payload["results"]

    diffs: List[str] = []
    total = len(results)

    for entry in results:
        sql_name = entry.get("sql_name", "<unknown>")
        dbs = entry.get("databases", {})
        oracle = dbs.get("oracle")
        sqlite = dbs.get("sqlite")

        if oracle is None or sqlite is None:
            diffs.append(f"{sql_name}: missing database result")
            if len(diffs) >= args.max_diffs:
                break
            continue

        if oracle.get("status") != sqlite.get("status"):
            diffs.append(
                f"{sql_name}: status mismatch: {oracle.get('status')} vs {sqlite.get('status')}"
            )
            if len(diffs) >= args.max_diffs:
                break
            continue

        if oracle.get("status") != "ok":
            continue

        rows_ok, reason = _compare_rows(
            oracle.get("rows", []),
            sqlite.get("rows", []),
            args.mode,
            args.eps,
        )
        if not rows_ok:
            diffs.append(f"{sql_name}: {reason}")
            if len(diffs) >= args.max_diffs:
                break

    print(f"Total: {total}")
    if diffs:
        print(f"Mismatch: {len(diffs)} (showing up to {args.max_diffs})")
        for item in diffs:
            print(f"- {item}")
        return 2

    print("Mismatch: 0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
