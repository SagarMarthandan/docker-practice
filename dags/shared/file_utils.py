"""File utility helpers shared across DAGs."""

import os


def remove_file(file_path: str) -> None:
    """Remove a file if it exists, printing a confirmation."""
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"Cleaned up: {file_path}")
