from __future__ import annotations

import argparse
from pathlib import Path

from _db_workflow import (
    copy_workspace_git_changes_to_changes,
    copy_workspace_path_to_changes,
    workspace_changes_root,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Copy a file or directory from `supabase/workspace/schemas` into "
            "`supabase/workspace/changes`, preserving the relative path."
        )
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--source-path")
    group.add_argument(
        "--git-changes",
        action="store_true",
        help="Copy every uncommitted file under `supabase/workspace/schemas` into `changes`.",
    )
    args = parser.parse_args()

    if args.git_changes:
        destinations = copy_workspace_git_changes_to_changes()
    else:
        destinations = copy_workspace_path_to_changes(Path(args.source_path))

    if not destinations:
        print("No files copied.")
        return

    if len(destinations) == 1:
        print(f"Copied to {destinations[0]}")
        return
    print(f"Copied {len(destinations)} files under {workspace_changes_root()}")


if __name__ == "__main__":
    main()
