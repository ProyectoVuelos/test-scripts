import json
import logging
import argparse
from pathlib import Path
from datetime import datetime
import config


def main():
    config.setup_logging()
    parser = argparse.ArgumentParser(
        description="Prepare flight timelines from a summary file."
    )
    parser.add_argument(
        "run_directory",
        help="Path to the 'run_...' directory containing the summary file.",
    )
    args = parser.parse_args()

    run_dir = Path(args.run_directory)
    summary_file = next(run_dir.glob("summaries/flights_summary_*.json"), None)

    if not summary_file or not summary_file.is_file():
        logging.critical(f"Summary file not found in {run_dir}.")
        return

    logging.info(f"Reading summaries from: {summary_file}")
    with open(summary_file, "r", encoding="utf-8") as f:
        summaries = json.load(f)

    timelines = []
    for summary in summaries:
        fr24_id = summary.get("fr24_id")
        flight_number = summary.get("flight") or summary.get("callsign")
        first_seen_str = summary.get("first_seen")
        last_seen_str = summary.get("last_seen")

        if not all([fr24_id, flight_number, first_seen_str, last_seen_str]):
            continue

        try:
            start_ts = int(
                datetime.fromisoformat(
                    first_seen_str.replace("Z", "+00:00")
                ).timestamp()
            )
            end_ts = int(
                datetime.fromisoformat(last_seen_str.replace("Z", "+00:00")).timestamp()
            )

            timelines.append(
                {
                    "fr24_id": fr24_id,
                    "flight_number": flight_number,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                }
            )
        except (ValueError, TypeError):
            continue

    output_file = run_dir / "flight_timelines.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(timelines, f, indent=2)

    logging.info(f"✅ Timelines prepared for {len(timelines)} flights.")
    logging.info(f"-> Saved to {output_file}")


if __name__ == "__main__":
    main()
