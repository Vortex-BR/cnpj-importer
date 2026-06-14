from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from app.normalization import strip_accents


@dataclass(frozen=True)
class CategoryRule:
    name: str
    code_prefixes: tuple[str, ...]
    keywords: tuple[str, ...]


class CategoryClassifier:
    def __init__(self, rules: list[CategoryRule], fallback: str = "Outros") -> None:
        self.rules = rules
        self.fallback = fallback

    @classmethod
    def from_yaml(cls, path: str | Path) -> "CategoryClassifier":
        with Path(path).open("r", encoding="utf-8") as config_file:
            payload = yaml.safe_load(config_file) or {}
        rules = [
            CategoryRule(
                name=item["name"],
                code_prefixes=tuple(str(prefix) for prefix in item.get("code_prefixes", [])),
                keywords=tuple(
                    strip_accents(str(keyword)).casefold() for keyword in item.get("keywords", [])
                ),
            )
            for item in payload.get("categories", [])
        ]
        return cls(rules, str(payload.get("fallback", "Outros")))

    def classify(self, code: str | None, description: str | None) -> str:
        normalized_code = (code or "").strip()
        normalized_description = strip_accents(description or "").casefold()
        for rule in self.rules:
            code_match = any(normalized_code.startswith(prefix) for prefix in rule.code_prefixes)
            keyword_match = any(
                keyword in normalized_description
                or (
                    len(keyword) > 4
                    and keyword[-1] in {"a", "o"}
                    and keyword[:-1] in normalized_description
                )
                for keyword in rule.keywords
            )
            if code_match or keyword_match:
                return rule.name
        return self.fallback
