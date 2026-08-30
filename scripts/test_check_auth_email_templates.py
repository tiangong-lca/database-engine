#!/usr/bin/env python3
"""Unit tests for check_auth_email_templates.py."""

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from check_auth_email_templates import EXPECTED_CONTENT_PATH, validate


VALID_TEMPLATE = """\
<a href="{{ .ConfirmationURL }}">Reset password</a>
<a href="{{ .ConfirmationURL }}">{{ .ConfirmationURL }}</a>
"""


class AuthEmailTemplateContractTests(unittest.TestCase):
    def make_repo(
        self,
        *,
        config_body: str | None = None,
        template_body: str | None = VALID_TEMPLATE,
    ) -> Path:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        (root / "supabase").mkdir()
        (root / "supabase" / "config.toml").write_text(
            config_body
            or f'''[auth.email.template.recovery]\nsubject = "Reset password"\ncontent_path = "{EXPECTED_CONTENT_PATH}"\n''',
            encoding="utf-8",
        )
        if template_body is not None:
            (root / "supabase" / "templates").mkdir()
            (root / "supabase" / "templates" / "recovery.html").write_text(
                template_body,
                encoding="utf-8",
            )
        return root

    def test_accepts_the_canonical_contract(self) -> None:
        self.assertEqual(validate(self.make_repo()), [])

    def test_requires_the_recovery_config_section(self) -> None:
        errors = validate(self.make_repo(config_body="[auth.email]\nenable_signup = true\n"))
        self.assertEqual(
            errors,
            ["supabase/config.toml must define [auth.email.template.recovery]"],
        )

    def test_requires_the_exact_path_subject_and_template_file(self) -> None:
        errors = validate(
            self.make_repo(
                config_body='''[auth.email.template.recovery]\nsubject = ""\ncontent_path = "./wrong.html"\n''',
                template_body=None,
            )
        )
        self.assertIn(f"recovery content_path must be {EXPECTED_CONTENT_PATH}", errors)
        self.assertIn("recovery subject must be a non-empty string", errors)
        self.assertIn("recovery template is missing: supabase/templates/recovery.html", errors)

    def test_requires_button_and_copyable_links_with_the_same_target(self) -> None:
        errors = validate(
            self.make_repo(template_body='<a href="https://example.com">Reset password</a>')
        )
        self.assertIn("recovery template must have a button-style ConfirmationURL link", errors)
        self.assertIn("recovery template must show ConfirmationURL as a copyable link", errors)

    def test_rejects_hand_built_magic_links(self) -> None:
        errors = validate(
            self.make_repo(
                template_body=VALID_TEMPLATE
                + '<a href="/verify?token={{ .TokenHash }}&type=magiclink">Legacy</a>'
            )
        )
        self.assertEqual(errors, ["recovery template must not hand-build a magic-link URL"])


if __name__ == "__main__":
    unittest.main()
