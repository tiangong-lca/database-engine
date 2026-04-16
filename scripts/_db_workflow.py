from __future__ import annotations

import argparse
import hashlib
import os
import re
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SUPABASE_ROOT = REPO_ROOT / "supabase"
MODEL_SCHEMA_ROOT = SUPABASE_ROOT / "model" / "schemas"
MIGRATIONS_ROOT = SUPABASE_ROOT / "migrations"
DEFAULT_ENVIRONMENT = "dev"
DEFAULT_SCHEMA_NAMES = ("public",)
SUPPORTED_ENVS = {"dev", "main", "local"}
SQL_NAME_RE = r'(?:(?:"[^"]+"|\w+)\.)?(?:"[^"]+"|\w+)'
DOLLAR_QUOTE_RE = re.compile(r"\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$")
REMOTE_WORKSPACE_ROOT = SUPABASE_ROOT / "workspace"
WORKSPACE_SCHEMAS_ROOT = REMOTE_WORKSPACE_ROOT / "schemas"
WORKSPACE_CHANGES_ROOT = REMOTE_WORKSPACE_ROOT / "changes"
REMOTE_SCHEMA_FILE = REMOTE_WORKSPACE_ROOT / "remote_schema.sql"


def repo_root() -> Path:
    return REPO_ROOT


def validate_environment(environment: str) -> None:
    if environment not in SUPPORTED_ENVS:
        raise RuntimeError(f"Unsupported environment: {environment}")


def safe_name(name: str) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*\s]+', "_", name.strip())
    cleaned = cleaned.strip(" ._")
    return cleaned or "unnamed"


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def remove_dir_safe(path: Path) -> None:
    if not path.exists():
        return
    resolved = path.resolve()
    root = repo_root().resolve()
    if root not in resolved.parents and resolved != root:
        raise RuntimeError(f"Refusing to remove path outside repo root: {resolved}")
    shutil.rmtree(resolved)


def read_env_value(path: Path, name: str) -> str:
    pattern = re.compile(rf"^\s*{re.escape(name)}\s*=\s*(.+?)\s*$")
    for line in path.read_text(encoding="utf-8").splitlines():
        match = pattern.match(line)
        if not match:
            continue
        value = match.group(1).strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            return value[1:-1]
        return value
    raise RuntimeError(f"Missing {name} in {path}")


def resolve_db_url(environment: str, explicit_db_url: str | None) -> str:
    validate_environment(environment)
    if explicit_db_url:
        return explicit_db_url
    env_file = repo_root() / f".env.supabase.{environment}.local"
    if not env_file.exists():
        raise RuntimeError(f"Missing env file: {env_file}")
    return read_env_value(env_file, "SUPABASE_DB_URL")


def run_command(command: list[str]) -> None:
    completed = subprocess.run(command, cwd=repo_root())
    if completed.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {completed.returncode}: {' '.join(command)}")


def resolve_supabase_cli_command() -> list[str]:
    override = os.environ.get("SUPABASE_CLI")
    if override:
        return [override]

    direct = shutil.which("supabase")
    if direct:
        return [direct]

    local_appdata = os.environ.get("LOCALAPPDATA")
    if local_appdata:
        bundled = Path(local_appdata) / "Programs" / "SupabaseCLI" / "bin" / "supabase.exe"
        if bundled.exists():
            return [str(bundled)]

    raise RuntimeError(
        "Supabase CLI not found. Install the standalone CLI or set SUPABASE_CLI to the executable path."
    )


def resolve_git_command() -> list[str]:
    direct = shutil.which("git")
    if direct:
        return [direct]

    windows_git = Path("C:/Program Files/Git/cmd/git.exe")
    if windows_git.exists():
        return [str(windows_git)]

    raise RuntimeError("Git not found.")


def resolve_schema_list(schemas: list[str] | str | None) -> list[str]:
    if schemas is None:
        return list(DEFAULT_SCHEMA_NAMES)
    if isinstance(schemas, str):
        return [schemas]
    return list(schemas)


def resolve_remote_schema_path(schema_file: Path | None) -> Path:
    return schema_file or REMOTE_SCHEMA_FILE


def remote_workspace_root() -> Path:
    return REMOTE_WORKSPACE_ROOT


def workspace_changes_root() -> Path:
    return WORKSPACE_CHANGES_ROOT


def workspace_schemas_root() -> Path:
    return WORKSPACE_SCHEMAS_ROOT


def reset_workspace_generated_content(workspace_root: Path) -> None:
    for name in ("global", "schemas"):
        remove_dir_safe(workspace_root / name)


def export_remote_schema(
    environment: str,
    db_url: str | None = None,
    schema_file: Path | None = None,
    schemas: list[str] | None = None,
) -> Path:
    resolved_db_url = resolve_db_url(environment, db_url)
    schema_path = resolve_remote_schema_path(schema_file)
    ensure_parent(schema_path)
    schema_list = resolve_schema_list(schemas)
    temp_schema_path = schema_path.with_name(f"{schema_path.stem}.tmp{schema_path.suffix}")

    command = [
        *resolve_supabase_cli_command(),
        "db",
        "dump",
        "--db-url",
        resolved_db_url,
        "--file",
        str(temp_schema_path),
        "--schema",
        ",".join(schema_list),
    ]

    try:
        run_command(command)
    except Exception:
        if temp_schema_path.exists():
            temp_schema_path.unlink()
        raise

    temp_schema_path.replace(schema_path)

    return schema_path


def split_sql_statements(sql_text: str) -> list[str]:
    filtered_lines = []
    for line in sql_text.splitlines():
        trimmed = line.lstrip()
        if trimmed.startswith("\\restrict") or trimmed.startswith("\\unrestrict"):
            continue
        filtered_lines.append(line)
    text = "\n".join(filtered_lines)

    statements: list[str] = []
    buffer: list[str] = []
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False
    in_dollar_quote: str | None = None

    i = 0
    while i < len(text):
        ch = text[i]
        nxt = text[i + 1] if i + 1 < len(text) else ""

        if in_dollar_quote:
            if text.startswith(in_dollar_quote, i):
                buffer.append(in_dollar_quote)
                i += len(in_dollar_quote)
                in_dollar_quote = None
            else:
                buffer.append(ch)
                i += 1
            continue

        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
                buffer.append(ch)
            i += 1
            continue

        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        if not in_single and not in_double:
            dollar_match = DOLLAR_QUOTE_RE.match(text, i)
            if dollar_match:
                in_dollar_quote = dollar_match.group(0)
                buffer.append(in_dollar_quote)
                i += len(in_dollar_quote)
                continue
            if ch == "-" and nxt == "-":
                in_line_comment = True
                i += 2
                continue
            if ch == "/" and nxt == "*":
                in_block_comment = True
                i += 2
                continue

        if ch == "'" and not in_double:
            if in_single and nxt == "'":
                buffer.extend([ch, nxt])
                i += 2
                continue
            in_single = not in_single
            buffer.append(ch)
            i += 1
            continue

        if ch == '"' and not in_single:
            if in_double and nxt == '"':
                buffer.extend([ch, nxt])
                i += 2
                continue
            in_double = not in_double
            buffer.append(ch)
            i += 1
            continue

        if ch == ";" and not in_single and not in_double:
            buffer.append(ch)
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
            i += 1
            continue

        buffer.append(ch)
        i += 1

    tail = "".join(buffer).strip()
    if tail:
        statements.append(tail)
    return statements


def unquote_identifier(token: str) -> str:
    token = token.strip()
    if len(token) >= 2 and token[0] == token[-1] == '"':
        return token[1:-1].replace('""', '"')
    return token


def split_qualified_name(token: str) -> tuple[str | None, str]:
    parts: list[str] = []
    current: list[str] = []
    in_double = False
    for ch in token:
        if ch == '"':
            in_double = not in_double
            current.append(ch)
            continue
        if ch == "." and not in_double:
            parts.append("".join(current).strip())
            current = []
            continue
        current.append(ch)
    if current:
        parts.append("".join(current).strip())
    cleaned = [unquote_identifier(part) for part in parts if part]
    if len(cleaned) == 1:
        return None, cleaned[0]
    return cleaned[0], cleaned[1]


def table_root(schema_name: str, table_name: str) -> Path:
    return Path("schemas") / safe_name(schema_name) / "tables" / safe_name(table_name)


def object_root(schema_name: str, object_type: str, object_name: str) -> Path:
    return Path("schemas") / safe_name(schema_name) / object_type / safe_name(object_name)


def statement_destinations(statement: str) -> list[Path]:
    text = statement.strip()

    def match(pattern: str) -> re.Match[str] | None:
        return re.match(pattern, text, flags=re.IGNORECASE | re.DOTALL)

    if match(r"^set\s+"):
        return [Path("global") / "preamble.sql"]

    if match(r"^select\s+pg_catalog\.set_config\s*\("):
        return [Path("global") / "preamble.sql"]

    m = match(r'^create\s+schema(?:\s+if\s+not\s+exists)?\s+(?P<name>"[^"]+"|\w+)')
    if m:
        return [Path("schemas") / safe_name(unquote_identifier(m.group("name"))) / "schema.sql"]

    m = match(r'^alter\s+schema\s+(?P<name>"[^"]+"|\w+)\s+')
    if m:
        return [Path("schemas") / safe_name(unquote_identifier(m.group("name"))) / "schema.sql"]

    m = match(r'^(grant|revoke)\s+.+?\s+on\s+schema\s+(?P<name>"[^"]+"|\w+)\s+')
    if m:
        return [Path("schemas") / safe_name(unquote_identifier(m.group("name"))) / "schema.sql"]

    m = match(r'^comment\s+on\s+schema\s+(?P<name>"[^"]+"|\w+)\s+')
    if m:
        return [Path("schemas") / safe_name(unquote_identifier(m.group("name"))) / "schema.sql"]

    m = match(r'^create\s+extension(?:\s+if\s+not\s+exists)?\s+(?P<name>"[^"]+"|\w+)')
    if m:
        return [Path("global") / "extensions" / f"{safe_name(unquote_identifier(m.group('name')))}.sql"]

    m = match(rf'^create\s+(?:or\s+replace\s+)?function\s+(?P<name>{SQL_NAME_RE})\s*\(')
    if m:
        schema_name, name = split_qualified_name(m.group("name"))
        return [object_root(schema_name or "public", "functions", name) / "definition.sql"]

    m = match(rf'^alter\s+function\s+(?P<name>{SQL_NAME_RE})\s*\(')
    if m:
        schema_name, name = split_qualified_name(m.group("name"))
        return [object_root(schema_name or "public", "functions", name) / "definition.sql"]

    m = match(rf'^(grant|revoke)\s+.+?\s+on\s+function\s+(?P<name>{SQL_NAME_RE})\s*\(')
    if m:
        schema_name, name = split_qualified_name(m.group("name"))
        return [object_root(schema_name or "public", "functions", name) / "definition.sql"]

    m = match(rf'^create\s+table(?:\s+if\s+not\s+exists)?\s+(?P<name>{SQL_NAME_RE})(?=\s|\()')
    if m:
        schema_name, name = split_qualified_name(m.group("name"))
        return [table_root(schema_name or "public", name) / "table.sql"]

    m = match(rf'^alter\s+table\s+(?:only\s+)?(?P<name>{SQL_NAME_RE})(?=\s|;)')
    if m:
        schema_name, name = split_qualified_name(m.group("name"))
        return [table_root(schema_name or "public", name) / "table.sql"]

    m = match(rf'^(grant|revoke)\s+.+?\s+on\s+table\s+(?P<name>{SQL_NAME_RE})(?=\s|;)')
    if m:
        schema_name, name = split_qualified_name(m.group("name"))
        return [table_root(schema_name or "public", name) / "table.sql"]

    m = match(rf'^comment\s+on\s+table\s+(?P<name>{SQL_NAME_RE})(?=\s|;)')
    if m:
        schema_name, name = split_qualified_name(m.group("name"))
        return [table_root(schema_name or "public", name) / "table.sql"]

    m = match(rf'^create\s+policy\s+(?P<policy>"[^"]+"|\w+)\s+on\s+(?P<name>{SQL_NAME_RE})(?=\s|;)')
    if m:
        schema_name, name = split_qualified_name(m.group("name"))
        policy = safe_name(unquote_identifier(m.group("policy")))
        return [table_root(schema_name or "public", name) / "policies" / f"{policy}.sql"]

    m = match(rf'^drop\s+policy(?:\s+if\s+exists)?\s+(?P<policy>"[^"]+"|\w+)\s+on\s+(?P<name>{SQL_NAME_RE})(?=\s|;)')
    if m:
        schema_name, name = split_qualified_name(m.group("name"))
        policy = safe_name(unquote_identifier(m.group("policy")))
        return [table_root(schema_name or "public", name) / "policies" / f"{policy}.sql"]

    m = match(rf'^alter\s+policy\s+(?P<policy>"[^"]+"|\w+)\s+on\s+(?P<name>{SQL_NAME_RE})(?=\s|;)')
    if m:
        schema_name, name = split_qualified_name(m.group("name"))
        policy = safe_name(unquote_identifier(m.group("policy")))
        return [table_root(schema_name or "public", name) / "policies" / f"{policy}.sql"]

    m = match(rf'^create\s+(?:or\s+replace\s+)?trigger\s+(?P<trigger>"[^"]+"|\w+)\s+.+?\s+on\s+(?P<name>{SQL_NAME_RE})(?=\s|;)')
    if m:
        schema_name, name = split_qualified_name(m.group("name"))
        trigger = safe_name(unquote_identifier(m.group("trigger")))
        return [table_root(schema_name or "public", name) / "triggers" / f"{trigger}.sql"]

    m = match(rf'^drop\s+trigger(?:\s+if\s+exists)?\s+(?P<trigger>"[^"]+"|\w+)\s+on\s+(?P<name>{SQL_NAME_RE})(?=\s|;)')
    if m:
        schema_name, name = split_qualified_name(m.group("name"))
        trigger = safe_name(unquote_identifier(m.group("trigger")))
        return [table_root(schema_name or "public", name) / "triggers" / f"{trigger}.sql"]

    m = match(rf'^create\s+(?:unique\s+)?index\s+(?P<index>"[^"]+"|\w+)\s+on\s+(?P<name>{SQL_NAME_RE})(?=\s|;)')
    if m:
        schema_name, name = split_qualified_name(m.group("name"))
        index_name = safe_name(unquote_identifier(m.group("index")))
        return [table_root(schema_name or "public", name) / "indexes" / f"{index_name}.sql"]

    m = match(rf'^create\s+type\s+(?P<name>{SQL_NAME_RE})(?=\s|;)')
    if m:
        schema_name, name = split_qualified_name(m.group("name"))
        return [object_root(schema_name or "public", "types", name) / "definition.sql"]

    m = match(rf'^alter\s+type\s+(?P<name>{SQL_NAME_RE})(?=\s|;)')
    if m:
        schema_name, name = split_qualified_name(m.group("name"))
        return [object_root(schema_name or "public", "types", name) / "definition.sql"]

    m = match(rf'^(grant|revoke)\s+.+?\s+on\s+sequence\s+(?P<name>{SQL_NAME_RE})(?=\s|;)')
    if m:
        schema_name, name = split_qualified_name(m.group("name"))
        return [object_root(schema_name or "public", "sequences", name) / "definition.sql"]

    m = match(rf'^create\s+materialized\s+view\s+(?P<name>{SQL_NAME_RE})(?=\s|;)')
    if m:
        schema_name, name = split_qualified_name(m.group("name"))
        return [object_root(schema_name or "public", "materialized_views", name) / "definition.sql"]

    m = match(rf'^alter\s+materialized\s+view\s+(?P<name>{SQL_NAME_RE})(?=\s|;)')
    if m:
        schema_name, name = split_qualified_name(m.group("name"))
        return [object_root(schema_name or "public", "materialized_views", name) / "definition.sql"]

    m = match(rf'^create\s+(?:or\s+replace\s+)?view\s+(?P<name>{SQL_NAME_RE})(?=\s|;)')
    if m:
        schema_name, name = split_qualified_name(m.group("name"))
        return [object_root(schema_name or "public", "views", name) / "definition.sql"]

    m = match(rf'^alter\s+view\s+(?P<name>{SQL_NAME_RE})(?=\s|;)')
    if m:
        schema_name, name = split_qualified_name(m.group("name"))
        return [object_root(schema_name or "public", "views", name) / "definition.sql"]

    if match(r"^alter\s+default\s+privileges\s+"):
        return [Path("global") / "default_privileges.sql"]

    digest = hashlib.sha1(text.encode("utf-8")).hexdigest()[:10]
    return [Path("global") / "other" / f"{digest}.sql"]


def write_workspace_files(workspace_root: Path, statements: list[str]) -> None:
    files: dict[Path, list[str]] = {}

    for statement in statements:
        for relative_path in statement_destinations(statement):
            absolute_path = workspace_root / relative_path
            files.setdefault(absolute_path, []).append(statement.rstrip() + "\n")

    for path, chunks in sorted(files.items()):
        ensure_parent(path)
        path.write_text("\n".join(chunks).strip() + "\n", encoding="utf-8")


def build_schema_workspace(
    environment: str,
    db_url: str | None = None,
    schemas: list[str] | None = None,
) -> Path:
    schema_path = export_remote_schema(environment, db_url=db_url, schemas=schemas)
    workspace_root = remote_workspace_root()
    workspace_root.mkdir(parents=True, exist_ok=True)
    reset_workspace_generated_content(workspace_root)

    sql_text = schema_path.read_text(encoding="utf-8")
    statements = split_sql_statements(sql_text)
    write_workspace_files(workspace_root, statements)
    return workspace_root


def migration_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
    return slug or "change"


def model_metadata(relative_path: Path) -> dict[str, str]:
    normalized = relative_path.as_posix()
    patterns = [
        (r"^([^/]+)/functions/([^/]+)/definition\.sql$", ("function", "schema", "name")),
        (r"^([^/]+)/views/([^/]+)/definition\.sql$", ("view", "schema", "name")),
        (r"^([^/]+)/materialized_views/([^/]+)/definition\.sql$", ("materialized_view", "schema", "name")),
        (r"^([^/]+)/tables/([^/]+)/policies/([^/]+)\.sql$", ("policy", "schema", "table", "name")),
        (r"^([^/]+)/tables/([^/]+)/triggers/([^/]+)\.sql$", ("trigger", "schema", "table", "name")),
    ]
    for pattern, keys in patterns:
        match = re.match(pattern, normalized, flags=re.IGNORECASE)
        if not match:
            continue
        values = dict(zip(keys[1:], match.groups()))
        values["kind"] = keys[0]
        return values
    raise RuntimeError(f"Unsupported model path: {relative_path}")


def extract_name(content: str, pattern: str) -> str:
    match = re.search(pattern, content, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise RuntimeError("Could not extract object name from model content")
    return unquote_identifier(match.group("name"))


def resolve_migration_source_relative_path(source_path: Path) -> Path:
    resolved_source = source_path.resolve()
    roots = (
        MODEL_SCHEMA_ROOT.resolve(),
        WORKSPACE_CHANGES_ROOT.resolve(),
    )
    for root in roots:
        try:
            return resolved_source.relative_to(root)
        except ValueError:
            continue
    allowed = " or ".join(str(root) for root in roots)
    raise RuntimeError(f"Migration source path must be inside {allowed}")


def resolve_workspace_source_relative_path(source_path: Path) -> Path:
    resolved_source = source_path.resolve()
    try:
        return resolved_source.relative_to(WORKSPACE_SCHEMAS_ROOT.resolve())
    except ValueError as exc:
        raise RuntimeError(f"Source path must be inside {WORKSPACE_SCHEMAS_ROOT}") from exc


def copy_workspace_path_to_changes(source_path: Path) -> list[Path]:
    resolved_source = source_path.resolve()
    if not resolved_source.exists():
        raise RuntimeError(f"Source path does not exist: {resolved_source}")

    destination_root = workspace_changes_root()
    destination_root.mkdir(parents=True, exist_ok=True)

    copied_paths: list[Path] = []

    if resolved_source.is_file():
        relative_source = resolve_workspace_source_relative_path(resolved_source)
        destination_path = destination_root / relative_source
        ensure_parent(destination_path)
        shutil.copy2(resolved_source, destination_path)
        copied_paths.append(destination_path)
        return copied_paths

    resolve_workspace_source_relative_path(resolved_source)
    for path in sorted(resolved_source.rglob("*")):
        if not path.is_file():
            continue
        relative_path = path.relative_to(WORKSPACE_SCHEMAS_ROOT.resolve())
        destination_path = destination_root / relative_path
        ensure_parent(destination_path)
        shutil.copy2(path, destination_path)
        copied_paths.append(destination_path)

    if not copied_paths:
        raise RuntimeError(f"No files found under source directory: {resolved_source}")

    return copied_paths


def workspace_git_changed_files() -> list[Path]:
    workspace_root = workspace_schemas_root().resolve()
    repo = repo_root().resolve()
    command = [
        *resolve_git_command(),
        "-c",
        f"safe.directory={repo.as_posix()}",
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
        "--",
        str(workspace_root),
    ]
    completed = subprocess.run(command, cwd=repo, capture_output=True)
    if completed.returncode != 0:
        stderr = completed.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(stderr or "Failed to inspect git status.")

    changed_files: list[Path] = []
    entries = completed.stdout.split(b"\x00")
    i = 0
    while i < len(entries):
        entry = entries[i]
        if not entry:
            i += 1
            continue

        record = entry.decode("utf-8", errors="replace")
        status = record[:2]
        path_text = record[3:]
        path = Path(path_text)

        if "D" not in status and path.is_file():
            changed_files.append(path)

        if status[0] in {"R", "C"} or status[1] in {"R", "C"}:
            i += 1

        i += 1

    unique_files = sorted({path.resolve() for path in changed_files})
    return unique_files


def copy_workspace_git_changes_to_changes() -> list[Path]:
    changed_files = workspace_git_changed_files()
    copied_paths: list[Path] = []
    for path in changed_files:
        copied_paths.extend(copy_workspace_path_to_changes(path))
    return copied_paths


def new_migration(name: str, source_path: Path, migrations_directory: Path | None = None) -> Path:
    resolved_source = source_path.resolve()
    relative_source = resolve_migration_source_relative_path(resolved_source)
    metadata = model_metadata(relative_source)
    content = resolved_source.read_text(encoding="utf-8").strip()

    kind = metadata["kind"]
    if kind in {"function", "view", "materialized_view"}:
        body = content
    elif kind == "policy":
        policy_name = extract_name(content, r'create\s+policy\s+(?P<name>"[^"]+"|\S+)')
        body = f'drop policy if exists "{policy_name}" on {metadata["schema"]}.{metadata["table"]};\n\n{content}'
    elif kind == "trigger":
        trigger_name = extract_name(content, r'create\s+(?:or\s+replace\s+)?trigger\s+(?P<name>"[^"]+"|\S+)')
        body = f'drop trigger if exists "{trigger_name}" on {metadata["schema"]}.{metadata["table"]};\n\n{content}'
    else:
        raise RuntimeError(f"Unsupported model kind: {kind}")

    migrations_dir = migrations_directory or MIGRATIONS_ROOT
    migrations_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{migration_slug(name)}.sql"
    migration_path = migrations_dir / filename
    migration_path.write_text(body.rstrip() + "\n", encoding="utf-8")
    return migration_path


def add_database_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--environment", default=DEFAULT_ENVIRONMENT, choices=sorted(SUPPORTED_ENVS))
    parser.add_argument("--db-url")
    parser.add_argument("--schemas", nargs="+", default=list(DEFAULT_SCHEMA_NAMES))


def add_common_args(parser: argparse.ArgumentParser) -> None:
    add_database_args(parser)
