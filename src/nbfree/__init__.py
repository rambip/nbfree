"""
Notebook Converter - Convert between Jupyter notebooks and Python files.
Stores Python files alongside notebooks with metadata in a special comment.
"""

import json
import hashlib
from pathlib import Path
from nbconvert.preprocessors import ExecutePreprocessor
import nbformat
from nbformat import NotebookNode
from dotenv import load_dotenv
from os import environ
from sys import exit


HEADER_COMMENT = "# %%\n"
HASH_PREFIX = "# %% NOTEBOOK_HASH="


class NotebookFile:
    """Class representing a Jupyter notebook file"""

    def __init__(self, path: Path, nb_data: NotebookNode):
        self.path = path
        self.nb_data = nb_data

    @staticmethod
    def from_code_cells(path: Path, raw_cells: list[str]):
        """Convert Python file to notebook format"""

        # Create cells from chunks
        cells = []
        for raw_cell in raw_cells:
            if raw_cell.startswith('"""') and raw_cell.endswith('"""'):
                # Remove only the triple quotes at the beginning and end
                content = raw_cell[3:-3]
                cell = {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": content,
                }
            else:
                # Code cell
                cell = {
                    "cell_type": "code",
                    "metadata": {},
                    "source": raw_cell,
                    "outputs": [],
                    "execution_count": None,
                }
            cells.append(cell)

        # Create notebook data
        nb_data = nbformat.from_dict(
            {
                "cells": cells,
                "metadata": {
                    "kernelspec": {
                        "display_name": "Python 3",
                        "language": "python",
                        "name": "python3",
                    },
                    "language_info": {"name": "python", "version": "3.10"},
                },
                "nbformat": 4,
                "nbformat_minor": 4,
            },
        )

        return NotebookFile(path, nb_data)  # type: ignore

    def execute(self, ep):
        ep.preprocess(self.nb_data)

    def write(self):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.nb_data, f, indent=2)

    def write_to_py(self, path: Path, hash):
        """Convert notebook to Python file format"""
        # Generate Python content from notebook cells
        chunks = []
        for cell in self.nb_data["cells"]:
            if cell["cell_type"] == "markdown":
                chunks.append(f'"""\n{"".join(cell["source"])}\n"""')
            elif cell["cell_type"] == "code":
                chunks.append("".join(cell["source"]))

        with open(path, "w", encoding="utf-8") as f:
            f.write(HASH_PREFIX)
            f.write(hash + "\n")
            f.write("\n# %%\n".join(chunks))

    def compute_hash(self):
        source = [c["source"] for c in self.nb_data["cells"]]
        return hashlib.md5(json.dumps(source).encode()).hexdigest()


def extract_hash_and_chunks(content):
    """Extract metadata from Python file content"""
    if content.startswith(HASH_PREFIX):
        lines = content.splitlines()
        hash = lines[0][len(HASH_PREFIX) :]
        content = "\n".join(lines[1:])
    else:
        hash = None

    parts = content.split(HEADER_COMMENT)
    return hash, [x.strip() for x in parts]


def load_python_file(path):
    """Load a Python file with metadata"""
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    return extract_hash_and_chunks(content)


def load_notebook(path):
    """Load a notebook file"""
    with open(path, encoding="utf-8") as f:
        data = nbformat.read(f, as_version=4)
    return NotebookFile(path, data)


def process_file_pair(stem, py_dir: Path, nb_dir: Path, ep):
    """Process a pair of notebook and Python files with the same stem.
    Returns the authoritative NotebookFile representing the current state."""

    py_path = py_dir / f"{stem}.py"
    nb_path = nb_dir / f"{stem}.ipynb"
    py_exists = py_path.exists()
    nb_exists = nb_path.exists()
    # Case: Only notebook exists
    if not py_exists and nb_exists:
        nb = load_notebook(nb_path)
        print(f"{nb_path} -> {py_path}")
        return nb

    # Case: Only Python file exists
    if not nb_exists and py_exists:
        hash_val, chunks = load_python_file(py_path)
        nb = NotebookFile.from_code_cells(nb_path, chunks)
        print(f"{py_path} -> {nb_path}")
        nb.execute(ep)
        return nb

    assert nb_exists and py_exists

    # Both files exist - determine which is authoritative
    ref_hash, chunks = load_python_file(py_path)
    nb = load_notebook(nb_path)

    # Create a notebook representation from Python chunks
    nb_from_py = NotebookFile.from_code_cells(nb_path, chunks)

    nb_hash = nb.compute_hash()
    py_hash = nb_from_py.compute_hash()

    # Determine what changed and which is authoritative
    if ref_hash is None:
        exit(
            f"File {py_path} exists but has not been created by this script."
            f"Please remove either {py_path} or {nb_path}"
            f"Hint: this script adds a special line `# %% NOTEBOOK_HASH='...'` at the start of the py file. Please don't remove it at the start of the py file. Please don't remove it."
        )
    elif ref_hash == nb_hash == py_hash:
        # No changes - either is fine
        return nb
    elif ref_hash == py_hash and ref_hash != nb_hash:
        # Notebook changed - notebook is authoritative
        return nb
        print(f"{nb_path} -> {py_path}")
    elif ref_hash == nb_hash and ref_hash != py_hash:
        # Python file changed - Python file is authoritative
        nb_from_py.execute(ep)
        print(f"{py_path} -> {nb_path}")
        return nb_from_py
    else:
        # Both changed independently - notebook is authoritative by default
        exit(
            f"Conflict for {stem}. Both {py_path} and {nb_exists} changed independently. Please remove one of them."
        )


def main():
    load_dotenv()
    """Convert all files that need conversion"""
    # Check for required environment variables
    if "SCRIPT_DIR" in environ:
        PY_DIR = Path(environ["SCRIPT_DIR"])
    else:
        exit(
            f"Error: SCRIPT_DIR environment variable not found. Please add it to the `.env` file in {Path().cwd()}"
        )
        return
    if "NOTEBOOK_DIR" in environ:
        NB_DIR = Path(environ["NOTEBOOK_DIR"])
    else:
        exit(
            f"Error: NOTEBOOK_DIR environment variable not found. Please add it to the `.env` file in {Path().cwd()}"
        )
        return

    # Find all Python and notebook files
    py_files = {file.stem: file for file in NB_DIR.glob("*.py")}
    nb_files = {file.stem: file for file in PY_DIR.glob("*.ipynb")}
    all_stems = set(py_files.keys()) | set(nb_files.keys())

    # Create execute preprocessor with custom kernel
    ep = ExecutePreprocessor(timeout=600)

    for stem in all_stems:
        # Get the authoritative notebook file
        nb = process_file_pair(stem, PY_DIR, NB_DIR, ep)

        # Write both files from the authoritative source
        if nb:
            nb.write()
            hash_val = nb.compute_hash()
            py_path = PY_DIR / f"{stem}.py"
            nb.write_to_py(py_path, hash_val)


if __name__ == "__main__":
    main()
