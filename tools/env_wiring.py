import ast
import json
import os
from pathlib import Path
from typing import Dict, List, Set, Tuple, Iterable, Optional

BOT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "bot_config.py"
REPO_ROOT = BOT_CONFIG_PATH.parent
ENV_ATLAS = REPO_ROOT / "ENV_ATLAS.env"
ENV_INDEX = REPO_ROOT / "ENV_INDEX.md"
ENV_WIRING = REPO_ROOT / "env_wiring.json"

# ----------------- AST helpers -----------------

def _attr_path(node: ast.AST, alias_map: Dict[str, str]) -> Optional[str]:
    if isinstance(node, ast.Name):
        return alias_map.get(node.id)
    if isinstance(node, ast.Attribute):
        base = _attr_path(node.value, alias_map)
        if base:
            return f"{base}.{node.attr}"
    return None


def _gather_wrapper_forwarders(fn: ast.FunctionDef) -> Set[str]:
    forwarders: Set[str] = set()
    param_names = {a.arg for a in fn.args.args}

    class WrapperVisitor(ast.NodeVisitor):
        def visit_Call(self, node: ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
                if func.value.id in {"_Env", "os"} and func.attr.startswith("get"):
                    if node.args and isinstance(node.args[0], ast.Name) and node.args[0].id in param_names:
                        forwarders.add(fn.name)
            self.generic_visit(node)

    WrapperVisitor().visit(fn)
    return forwarders


def extract_alias_map(module: ast.AST) -> Dict[str, str]:
    alias_map: Dict[str, str] = {"cfg": "cfg"}
    class AliasVisitor(ast.NodeVisitor):
        def visit_Assign(self, node: ast.Assign):
            if len(node.targets) != 1:
                return
            target = node.targets[0]
            if isinstance(target, ast.Name):
                value = node.value
                if isinstance(value, ast.Attribute):
                    base = _attr_path(value, alias_map)
                    if base:
                        alias_map[target.id] = base
            elif isinstance(target, ast.Attribute):
                # self._alias_map = {"A": "b"}
                if (
                    isinstance(target.value, ast.Name)
                    and target.value.id == "self"
                    and target.attr == "_alias_map"
                    and isinstance(node.value, ast.Dict)
                ):
                    for k, v in zip(node.value.keys, node.value.values):
                        if isinstance(k, ast.Constant) and isinstance(k.value, str) and isinstance(v, ast.Constant) and isinstance(v.value, str):
                            alias_map[k.value] = v.value
    for stmt in module.body:
        if isinstance(stmt, ast.ClassDef) and stmt.name == "BotConfig":
            for item in stmt.body:
                if isinstance(item, ast.FunctionDef) and item.name == "_init_aliases":
                    AliasVisitor().visit(item)
    return alias_map


class FromEnvAnalyzer(ast.NodeVisitor):
    def __init__(self, module: ast.AST) -> None:
        super().__init__()
        self.module = module
        self.env_reads: Dict[str, List[int]] = {}
        self.env_assignments: Dict[str, Set[str]] = {}
        self.name_sources: Dict[str, Set[str]] = {}
        self.alias_map: Dict[str, str] = {"cfg": "cfg"}
        self.wrapper_forwarders: Set[str] = set()

    def record_env(self, key: str, lineno: int) -> None:
        self.env_reads.setdefault(key, []).append(lineno)

    def _value_env_sources(self, node: ast.AST) -> Set[str]:
        sources: Set[str] = set()
        if isinstance(node, ast.Call):
            func = node.func
            # _Env.get*
            if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
                if func.value.id == "_Env" and node.args:
                    if isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                        key = node.args[0].value
                        sources.add(key)
                        self.record_env(key, node.lineno)
                if func.value.id == "os" and func.attr == "getenv" and node.args:
                    if isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                        key = node.args[0].value
                        sources.add(key)
                        self.record_env(key, node.lineno)
            # wrapper forwarders
            if isinstance(func, ast.Name) and func.id in self.wrapper_forwarders and node.args:
                if isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                    key = node.args[0].value
                    sources.add(key)
                    self.record_env(key, node.lineno)
            # traverse callable root (ex: (_Env.get_dict(...) or {}).items())
            sources.update(self._value_env_sources(func))
            # aggregate child args
            for arg in list(node.args) + [kw.value for kw in node.keywords]:
                sources.update(self._value_env_sources(arg))
            return sources
        if isinstance(node, ast.Attribute):
            return set(self._value_env_sources(node.value))
        if isinstance(node, ast.Name):
            return set(self.name_sources.get(node.id, set()))
        if isinstance(node, (ast.BinOp, ast.BoolOp, ast.UnaryOp, ast.IfExp, ast.Compare)):
            for child in ast.iter_child_nodes(node):
                sources.update(self._value_env_sources(child))
            return sources
        if isinstance(node, (ast.Tuple, ast.List, ast.Set, ast.Dict)):
            for child in ast.iter_child_nodes(node):
                sources.update(self._value_env_sources(child))
            return sources
        if isinstance(node, (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)):
            if isinstance(node, (ast.ListComp, ast.SetComp, ast.GeneratorExp)):
                sources.update(self._value_env_sources(node.elt))
            if isinstance(node, ast.DictComp):
                sources.update(self._value_env_sources(node.key))
                sources.update(self._value_env_sources(node.value))
            for gen in node.generators:
                sources.update(self._value_env_sources(gen.iter))
                for if_ in gen.ifs:
                    sources.update(self._value_env_sources(if_))
            return sources
        return sources

    def _handle_assignment(self, targets: List[ast.expr], value: ast.AST):
        env_sources = self._value_env_sources(value)
        # alias propagation first
        for target in targets:
            if isinstance(target, ast.Name):
                alias_path = _attr_path(value, self.alias_map)
                if alias_path:
                    self.alias_map[target.id] = alias_path
        if not env_sources:
            return
        for target in targets:
            if isinstance(target, ast.Name):
                self.name_sources[target.id] = set(env_sources)
            path = _attr_path(target, self.alias_map)
            if path:
                for key in env_sources:
                    self.env_assignments.setdefault(key, set()).add(path)

    def visit_FunctionDef(self, node: ast.FunctionDef):
        if node.name == "from_env":
            # collect wrapper forwarders inside function body
            for inner in node.body:
                if isinstance(inner, ast.FunctionDef):
                    self.wrapper_forwarders.update(_gather_wrapper_forwarders(inner))
            # seed alias map for names assigned from cfg.*
            for inner in node.body:
                if isinstance(inner, ast.Assign):
                    self._handle_assignment(inner.targets, inner.value)
            # now traverse fully
            for inner in node.body:
                self.visit(inner)
        # do not descend elsewhere

    def visit_Assign(self, node: ast.Assign):
        self._handle_assignment(node.targets, node.value)
        self.generic_visit(node.value)

    def visit_AnnAssign(self, node: ast.AnnAssign):
        targets = [node.target]
        self._handle_assignment(targets, node.value if node.value else ast.Constant(None))
        if node.value:
            self.generic_visit(node.value)


def analyze_from_env(module: ast.AST) -> FromEnvAnalyzer:
    analyzer = FromEnvAnalyzer(module)
    analyzer.alias_map.update(extract_alias_map(module))
    analyzer.visit(module)
    return analyzer


def load_ast() -> ast.AST:
    return ast.parse(BOT_CONFIG_PATH.read_text())

# ----------------- Usage scanning -----------------

def scan_usage(tokens: Set[str]) -> Dict[str, Set[str]]:
    """Retourne les occurrences (fichier:ligne) pour chaque token."""
    tokens = {t for t in tokens if t}
    hits: Dict[str, Set[str]] = {t: set() for t in tokens}
    for path in REPO_ROOT.rglob("*"):
        if path.is_dir():
            continue
        if ".git" in path.parts or path.name == "bot_config.py":
            continue
        if path.name in {"ENV_ATLAS.env", "ENV_INDEX.md", "env_wiring.json"}:
            continue
        try:
            content = path.read_text(errors="ignore")
        except Exception:
            continue
        for lineno, line in enumerate(content.splitlines(), 1):
            for token in tokens:
                if token in line:
                    rel = os.path.relpath(path, REPO_ROOT)
                    hits[token].add(f"{rel}:{lineno}")
    return hits


def classify_usage(paths: Iterable[str]) -> str:
    if not paths:
        return "NO_USAGE_FOUND"
    unique_files = {p for p in paths}
    if all("tests" in p or p.endswith("smoke_test_all.py") for p in unique_files):
        return "TEST_ONLY"
    return "USED"


# ----------------- Rendering -----------------


def render_env_atlas(data):
    lines = ["# ENV_ATLAS — généré automatiquement", "# Formats: get_bool TRUE={1,true,yes,on,y,t} FALSE={0,false,no,off,n,f}; get_list: CSV/JSON list; get_dict: JSON/Python dict; get_routes: JSON list of pairs or A:B;C:D", ""]
    for key in sorted(data.keys()):
        entry = data[key]
        lines.append(f"# READ: bot_config.py:L{','.join(map(str, sorted(set(entry['read_lines']))))}")
        lines.append(f"# WRITE: {', '.join(sorted(entry['cfg_paths'])) if entry['cfg_paths'] else '—'}")
        lines.append(f"# USED: {entry['status']}")
        for usage in sorted(entry['usage_locations']):
            lines.append(f"#   use: {usage}")
        lines.append(f"{key}=")
        lines.append("")
    return "\n".join(lines)


def render_index(data):
    lines = ["# ENV_INDEX", ""]
    lines.append(f"Total keys: {len(data)}. Source unique: bot_config.py::BotConfig.from_env.")
    lines.append("")
    lines.append("## Index par clé")
    lines.append("")
    for key in sorted(data.keys()):
        entry = data[key]
        lines.append(f"### {key}")
        lines.append(f"- READ: bot_config.py:L{','.join(map(str, sorted(set(entry['read_lines']))))}")
        lines.append(f"- WRITE: {', '.join(sorted(entry['cfg_paths'])) if entry['cfg_paths'] else '—'}")
        if entry['usage_locations']:
            lines.append("- USED:")
            for loc in sorted(entry['usage_locations']):
                lines.append(f"  - {loc}")
        else:
            lines.append("- USED: none")
        lines.append(f"- STATUS: {entry['status']}")
        lines.append("")
    return "\n".join(lines)


# ----------------- Main -----------------


def main():
    module = load_ast()
    analyzer = analyze_from_env(module)

    env_data: Dict[str, Dict[str, object]] = {}

    # prepare alias reverse map
    alias_rev: Dict[str, List[str]] = {}
    for alias, path in analyzer.alias_map.items():
        alias_rev.setdefault(path, []).append(alias)

    for key, cfg_paths in analyzer.env_assignments.items():
        env_data.setdefault(key, {
            "read_lines": analyzer.env_reads.get(key, []),
            "cfg_paths": set(),
            "usage_locations": set(),
            "status": "NO_USAGE_FOUND",
        })
        env_data[key]["cfg_paths"].update(cfg_paths)
    # ensure keys read but maybe not assigned
    for key, lines in analyzer.env_reads.items():
        env_data.setdefault(key, {
            "read_lines": lines,
            "cfg_paths": set(),
            "usage_locations": set(),
            "status": "NO_USAGE_FOUND",
        })
        env_data[key]["read_lines"] = lines

    token_map: Dict[str, Set[str]] = {}
    for entry in env_data.values():
        paths = set(entry["cfg_paths"])
        search_tokens: Set[str] = set()
        for p in paths:
            search_tokens.add("." + p)
            search_tokens.add(p)
            for alias in alias_rev.get(p, []):
                search_tokens.add(alias)
        token_map[id(entry)] = search_tokens

    all_tokens: Set[str] = set()
    for toks in token_map.values():
        all_tokens.update(toks)
    token_hits = scan_usage(all_tokens)

    for entry in env_data.values():
        usage_paths: Set[str] = set()
        for token in token_map[id(entry)]:
            usage_paths.update(token_hits.get(token, set()))
        entry["usage_locations"] = usage_paths
        entry["status"] = classify_usage({p.split(":")[0] for p in usage_paths})
        entry["cfg_paths"] = set(entry["cfg_paths"])

    atlas_content = render_env_atlas(env_data)
    index_content = render_index(env_data)

    ENV_ATLAS.write_text(atlas_content + "\n")
    ENV_INDEX.write_text(index_content + "\n")
    wiring_payload = {
        k: {
            "cfg_paths": sorted(list(v["cfg_paths"])),
            "used_in": sorted(list(v["usage_locations"])),
            "status": v["status"],
        }
        for k, v in sorted(env_data.items())
    }
    ENV_WIRING.write_text(json.dumps(wiring_payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
