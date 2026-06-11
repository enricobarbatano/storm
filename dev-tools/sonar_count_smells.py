#!/usr/bin/env python3
"""
Count SonarQube CODE_SMELL issues per Java class/file and export CSV.

Usage (example):
  # set SONAR_TOKEN in environment first (do NOT paste token in chat)
  $env:SONAR_TOKEN = 'YOUR_TOKEN'            # PowerShell (session)
  export SONAR_TOKEN='YOUR_TOKEN'            # bash
  python dev-tools/sonar_count_smells.py --sonar-url https://sonarcloud.io \
      --project-key org.apache.storm:storm --output stdout

The script reads `SONAR_TOKEN` from the environment and uses it as HTTP basic auth (token, empty password).
"""
from __future__ import print_function
import os
import sys
import argparse
import requests
import csv
from collections import defaultdict


def parse_args():
    parser = argparse.ArgumentParser(
        description="Count Sonar CODE_SMELL issues per Java class/file"
    )
    parser.add_argument("--sonar-url", required=True, help="SonarQube server URL (e.g. https://sonarcloud.io)")
    parser.add_argument("--project-key", required=True, help="Sonar project key (e.g. org.apache.storm:storm)")
    parser.add_argument("--output", default="reports/sonar_smells.csv",
                        help="Output CSV path or 'stdout' for console")
    parser.add_argument("--page-size", type=int, default=500, help="Sonar API page size (max 500)")
    return parser.parse_args()


def get_issues(sonar_url, project_key, token, page_size=500):
    session = requests.Session()
    session.auth = (token, "")
    api = sonar_url.rstrip("/") + "/api/issues/search"
    page = 1
    total = None
    while True:
        params = {
            "componentKeys": project_key,
            "types": "CODE_SMELL",
            "ps": page_size,
            "p": page,
            "resolved": "false",
        }
        resp = session.get(api, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if total is None:
            total = int(data.get("total", 0))
        issues = data.get("issues", [])
        if not issues:
            break
        for issue in issues:
            yield issue
        # pagination
        if page * page_size >= total:
            break
        page += 1


def normalize_component_to_file_path(component):
    if not component:
        return ""
    # typical format: 'module:key:src/main/java/.../File.java'
    if "src/" in component:
        idx = component.find("src/")
        return component[idx:]
    parts = component.split(":")
    return parts[-1] if parts else component


def main():
    args = parse_args()
    token = os.environ.get("SONAR_TOKEN")
    if not token:
        print("Errore: imposta la variabile d'ambiente SONAR_TOKEN con il tuo token Sonar (non incollarlo qui).", file=sys.stderr)
        sys.exit(2)

    counts = defaultdict(int)

    try:
        for issue in get_issues(args.sonar_url, args.project_key, token, args.page_size):
            comp = issue.get("component", "")
            file_path = normalize_component_to_file_path(comp)
            class_name = os.path.splitext(os.path.basename(file_path))[0]
            key = (class_name, file_path)
            counts[key] += 1
    except requests.HTTPError as e:
        print("Errore HTTP durante la chiamata a SonarQube:", e, file=sys.stderr)
        sys.exit(3)
    except Exception as e:
        print("Errore durante il recupero degli issue:", e, file=sys.stderr)
        sys.exit(4)

    items = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)

    if args.output.lower() == "stdout":
        print("class_name,file_path,smell_count")
        for (class_name, file_path), count in items:
            print(f"{class_name},{file_path},{count}")
        print("\nTop 20 classes:")
        for i, ((class_name, file_path), count) in enumerate(items[:20], start=1):
            print(f"{i:2d}. {class_name} ({count}) - {file_path}")
    else:
        out = args.output
        d = os.path.dirname(out)
        if d:
            os.makedirs(d, exist_ok=True)
        with open(out, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["class_name", "file_path", "smell_count"])
            for (class_name, file_path), count in items:
                writer.writerow([class_name, file_path, count])
        print(f"CSV salvato in {out}")


if __name__ == '__main__':
    main()
