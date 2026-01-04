"""Validation renforcée pour l'atlas d'environnement.

- Vérifie que toutes les clés lues par BotConfig.from_env sont présentes dans ENV_ATLAS.env.
- Vérifie que l'atlas ne contient pas de clés inconnues (hors allowlist LEGACY_ALLOWLIST).
"""
import ast
from pathlib import Path
from typing import Set

REPO_ROOT = Path(__file__).resolve().parent.parent
BOT_CONFIG_PATH = REPO_ROOT / "bot_config.py"
ENV_ATLAS_PATH = REPO_ROOT / "ENV_ATLAS.env"

# Compléter ce set si des clés historiques (non présentes dans BotConfig.from_env)
# doivent rester dans l'atlas pour compatibilité.
LEGACY_ALLOWLIST: Set[str] = set()


def _extract_env_keys_from_from_env(tree: ast.AST) -> Set[str]:
    keys: Set[str] = set()

    class WrapperVisitor(ast.NodeVisitor):
        def __init__(self):
            super().__init__()
            self.wrapper_forwarders: Set[str] = set()

        def visit_FunctionDef(self, node: ast.FunctionDef):
            if node.name == "from_env":
                for inner in node.body:
                    if isinstance(inner, ast.FunctionDef):
                        if self._is_env_forwarder(inner):
                            self.wrapper_forwarders.add(inner.name)
                self.generic_visit(node)

        def _is_env_forwarder(self, fn: ast.FunctionDef) -> bool:
            param_names = {a.arg for a in fn.args.args}
            for n in ast.walk(fn):
                if isinstance(n, ast.Call):
                    func = n.func
                    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
                        if func.value.id in {"_Env", "os"} and func.attr.startswith("get"):
                            if n.args and isinstance(n.args[0], ast.Name) and n.args[0].id in param_names:
                                return True
            return False

        def visit_Call(self, node: ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
                if func.value.id == "_Env" and node.args:
                    if isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                        keys.add(node.args[0].value)
                if func.value.id == "os" and func.attr == "getenv" and node.args:
                    if isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                        keys.add(node.args[0].value)
            if isinstance(func, ast.Name) and func.id in self.wrapper_forwarders and node.args:
                if isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                    keys.add(node.args[0].value)
            self.generic_visit(node)

    WrapperVisitor().visit(tree)
    return keys


def _load_atlas_keys(path: Path) -> Set[str]:
    keys: Set[str] = set()
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            keys.add(line.split("=", 1)[0])
    return keys


def main() -> None:
    tree = ast.parse(BOT_CONFIG_PATH.read_text())
    extracted_keys = _extract_env_keys_from_from_env(tree)
    atlas_keys = _load_atlas_keys(ENV_ATLAS_PATH)

    missing = extracted_keys - atlas_keys
    extra = atlas_keys - extracted_keys - LEGACY_ALLOWLIST

    if missing:
        raise SystemExit(f"Validation failed: missing in atlas {sorted(missing)}")
    if extra:
        raise SystemExit(f"Validation failed: extra keys not in from_env {sorted(extra)}")

    print(f"Validation succeeded: {len(atlas_keys)} keys align with from_env.")


if __name__ == "__main__":
    main()
