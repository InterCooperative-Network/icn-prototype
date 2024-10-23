import os


def tree_structure(startpath, max_depth=2):
    tree_str = ""
    start_depth = startpath.rstrip(os.path.sep).count(os.path.sep)
    for root, dirs, files in os.walk(startpath):
        depth = root.count(os.path.sep) - start_depth
        if depth > max_depth:
            continue
        indent = " " * 4 * depth
        tree_str += f"{indent}{os.path.basename(root)}/\n"
        sub_indent = " " * 4 * (depth + 1)
        for file in files:
            tree_str += f"{sub_indent}{file}\n"
    return tree_str


def concatenate_code_files(source_dir, output_file, extensions=None, max_depth=2):
    if extensions is None:
        extensions = [".py", ".rs", ".js", ".ts", ".c"]  # Only most relevant extensions

    # Generate the tree structure
    tree_str = tree_structure(source_dir, max_depth=max_depth)

    with open(output_file, "w", encoding="utf-8") as outfile:
        outfile.write(
            "# Project Directory Structure (up to depth {}):\n".format(max_depth)
        )
        outfile.write(tree_str)
        outfile.write("\n\n# Code Files Concatenation:\n\n")

        # Walk through the directory to find and concatenate code files
        for root, dirs, files in os.walk(source_dir):
            # Exclude temp and irrelevant directories, including 'icn_env'
            dirs[:] = [
                d
                for d in dirs
                if d
                not in [
                    "__pycache__",
                    ".git",
                    "node_modules",
                    "build",
                    "dist",
                    "venv",
                    ".idea",
                    "icn_env",
                ]
            ]

            for file in files:
                file_path = os.path.join(root, file)

                # Skip non-relevant extensions
                if not any(file.endswith(ext) for ext in extensions):
                    continue

                try:
                    # Skip files larger than 50 KB
                    if os.path.getsize(file_path) > 50 * 1024:
                        continue

                    # Check for binary content
                    with open(file_path, "rb") as binary_check:
                        if b"\0" in binary_check.read(1024):
                            continue

                    with open(
                        file_path, "r", encoding="utf-8", errors="ignore"
                    ) as infile:
                        outfile.write(f"\n\n# {'='*20} {file_path} {'='*20}\n\n")
                        outfile.write(infile.read())
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")

    print(f"Concatenation complete! Check the output file: {output_file}")


# Example usage
concatenate_code_files("/home/matt/icn-prototype", "ICN_code_dump.txt", max_depth=2)
