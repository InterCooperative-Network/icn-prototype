import os
import time

# Define directories and files to exclude
EXCLUDE_DIRS = ["__pycache__", ".git", "node_modules", "build", "dist", "venv", ".idea", "icn_env", "docs", "icn-docs"]
EXCLUDE_FILES = [".pyc", ".pyo", ".log", ".tmp", ".cache", ".DS_Store"]

def tree_structure(startpath, max_depth=3):
    """
    Generate a visual representation of the directory structure up to a specified depth,
    excluding irrelevant directories and files.
    """
    tree_str = ""
    start_depth = startpath.rstrip(os.path.sep).count(os.path.sep)

    for root, dirs, files in os.walk(startpath):
        depth = root.count(os.path.sep) - start_depth
        if depth > max_depth:
            continue

        # Exclude irrelevant directories
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]

        indent = " " * 4 * depth
        tree_str += f"{indent}{os.path.basename(root)}/\n"

        sub_indent = " " * 4 * (depth + 1)
        for file in files:
            if any(file.endswith(ext) for ext in EXCLUDE_FILES):
                continue
            tree_str += f"{sub_indent}{file}\n"

    return tree_str


def concatenate_code_files(source_dir, output_file, extensions=None, max_depth=3):
    """
    Concatenate code files from a directory, excluding cache files, documentation files,
    and irrelevant directories like 'icn-docs'.
    """
    if extensions is None:
        extensions = [".py", ".rs", ".js", ".ts", ".c"]

    # Remove the old output file if it exists
    if os.path.exists(output_file):
        os.remove(output_file)
        print(f"Removed old output file: {output_file}")

    # Generate the tree structure
    tree_str = tree_structure(source_dir, max_depth=max_depth)

    # Prepare file content and summary as strings
    content_str = ""
    summary_str = "\n# File Summary:\n"

    file_summary = []  # To store summary info for each file

    for root, dirs, files in os.walk(source_dir):
        # Exclude irrelevant directories
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]

        # Skip any directory containing 'icn-docs' or 'docs' in its path
        if "icn-docs" in root.split(os.path.sep) or "docs" in root.split(os.path.sep):
            continue

        for file in files:
            # Skip excluded file types
            if any(file.endswith(ext) for ext in EXCLUDE_FILES):
                continue

            file_path = os.path.join(root, file)

            # Only include files with relevant extensions
            if not any(file.endswith(ext) for ext in extensions):
                continue

            try:
                file_size = os.path.getsize(file_path)

                with open(file_path, "rb") as binary_check:
                    if b"\0" in binary_check.read(1024):
                        file_summary.append((file_path, file_size, "Skipped (Binary)"))
                        continue

                with open(file_path, "r", encoding="utf-8", errors="ignore") as infile:
                    last_modified = time.ctime(os.path.getmtime(file_path))
                    lang = file.split(".")[-1]

                    # File metadata
                    content_str += f"\n\n# {'='*60}\n"
                    content_str += f"# File: {file_path}\n"
                    content_str += f"# Size: {file_size} bytes\n"
                    content_str += f"# Last Modified: {last_modified}\n"
                    content_str += f"# Language: {lang}\n"
                    content_str += f"# {'='*60}\n\n"

                    # Syntax-highlighted code block
                    content_str += f"```{lang}\n"
                    file_content = infile.read()

                    # Ensure consistent indentation for readability
                    indented_content = "\n".join("    " + line for line in file_content.splitlines())
                    content_str += indented_content
                    content_str += "\n```\n"

                    file_summary.append((file_path, file_size, "Included"))
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
                file_summary.append((file_path, "N/A", f"Error: {e}"))

    # Construct the summary string
    for file_info in file_summary:
        summary_str += f"# {file_info[0]} - {file_info[1]} bytes - {file_info[2]}\n"

    # Write everything to the output file
    with open(output_file, "w", encoding="utf-8") as outfile:
        outfile.write(summary_str + "\n\n")
        outfile.write("# Project Directory Structure (up to depth {}):\n".format(max_depth))
        outfile.write(tree_str)
        outfile.write("\n\n# Code Files Concatenation:\n\n")
        outfile.write(content_str)

    print(f"Concatenation complete! Check the output file: {output_file}")


# Example usage
concatenate_code_files("/home/matt/icn-prototype", "ICN_code_dump.txt", max_depth=3)
