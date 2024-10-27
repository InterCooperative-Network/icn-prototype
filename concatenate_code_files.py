import os
import time

# Define directories and files to exclude
EXCLUDE_DIRS = ["__pycache__", ".git", "node_modules", "build", "dist", "venv", ".idea", "icn_env", "docs", "icn-docs", "output"]
EXCLUDE_FILES = [".pyc", ".pyo", ".log", ".tmp", ".cache", ".DS_Store"]

# Define context mapping for modules
MODULE_CONTEXT = {
    "blockchain": {
        "purpose": "Handles core blockchain logic, consensus mechanisms, and transaction validation.",
        "vision_alignment": (
            "This module ensures decentralized, cooperative-based transactions and consensus, "
            "forming the backbone of the ICN's transparent governance and trustless operations."
        ),
        "interaction": "Integrates with 'did' for identity verification and 'api' for interaction."
    },
    "did": {
        "purpose": "Manages Decentralized Identifiers (DIDs) for secure, privacy-preserving user identities.",
        "vision_alignment": (
            "Enables privacy-focused governance and secure user participation, aligning with the ICN's "
            "commitment to privacy, identity security, and cooperative integrity."
        ),
        "interaction": "Works with 'blockchain' for identity validation and 'api' for authentication."
    },
    "tests": {
        "purpose": "Ensures the reliability and performance of ICN components through comprehensive testing.",
        "vision_alignment": (
            "Maintains system integrity by validating that all modules perform as expected, "
            "supporting ICN’s stability and trustworthiness."
        ),
        "interaction": "Covers tests for 'blockchain,' 'did,' 'api,' and 'system' components."
    },
    "api": {
        "purpose": "Facilitates external interactions with ICN, enabling seamless integration and user interfaces.",
        "vision_alignment": (
            "Acts as the gateway for cooperative engagement, making ICN accessible to external applications "
            "and users, supporting cooperative growth and interoperability."
        ),
        "interaction": "Connects with 'blockchain' for transactions, 'did' for identity management."
    },
    "system": {
        "purpose": "Manages system-level operations, including node management and consensus monitoring.",
        "vision_alignment": (
            "Coordinates cooperative governance and resource sharing, supporting the decentralized operations "
            "of ICN nodes and decision-making processes."
        ),
        "interaction": "Works with 'blockchain' for block propagation and 'api' for control."
    }
}

# General project context
PROJECT_CONTEXT = (
    "The InterCooperative Network (ICN) is a decentralized cooperative management system designed to support "
    "global governance, privacy-preserving identity, and resource sharing. It uses blockchain technology for "
    "consensus and DIDs for secure identities, with modules designed for scalable, democratic interaction. "
    "The ICN promotes cooperative-based decision-making, transparent governance, and equitable resource distribution."
)

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

        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
        indent = " " * 4 * depth
        tree_str += f"{indent}{os.path.basename(root)}/\n"
        sub_indent = " " * 4 * (depth + 1)

        for file in files:
            if any(file.endswith(ext) for ext in EXCLUDE_FILES):
                continue
            tree_str += f"{sub_indent}{file}\n"

    return tree_str


def create_output_file(output_dir, module_name, project_structure, content_str):
    """
    Create a new output file for a specific module, including the project structure and context.
    """
    output_file = os.path.join(output_dir, f"{module_name}_dump.txt")
    with open(output_file, "w", encoding="utf-8") as outfile:
        # Add general project context and module-specific context
        outfile.write("# Project Context:\n")
        outfile.write(PROJECT_CONTEXT + "\n\n")
        
        context = MODULE_CONTEXT.get(module_name, {})
        module_overview = f"# Module: {module_name}\n"
        module_overview += f"# Purpose: {context.get('purpose', 'No specific context provided.')}\n"
        module_overview += f"# Vision Alignment: {context.get('vision_alignment', 'No vision context provided.')}\n"
        module_overview += f"# Interaction with Other Modules: {context.get('interaction', 'No interaction details provided.')}\n"
        outfile.write(module_overview)
        outfile.write("\n\n")

        # Add module-specific content
        outfile.write("# Code Files for Module: {}\n\n".format(module_name))
        outfile.write(content_str)

    print(f"Created output file: {output_file}")


def get_closest_module(file_path):
    """
    Determine the closest related module for unclassified files.
    """
    if "blockchain" in file_path:
        return "blockchain"
    elif "did" in file_path:
        return "did"
    elif "tests" in file_path:
        return "tests"
    elif "api" in file_path:
        return "api"
    elif "system" in file_path:
        return "system"
    else:
        return None


def concatenate_code_files(source_dir, max_depth=3, extensions=None):
    """
    Concatenate code files from a directory into logically grouped smaller output files.
    Each output includes the overall project structure, module overview, and content with context.
    """
    if extensions is None:
        extensions = [".py", ".rs", ".js", ".ts", ".c"]

    output_dir = os.path.join(source_dir, "output")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Generate the project structure overview
    project_structure = tree_structure(source_dir, max_depth=max_depth)

    module_contents = {}

    for root, dirs, files in os.walk(source_dir):
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]

        # Skip processing the output directory
        if output_dir in root:
            continue

        for file in files:
            if any(file.endswith(ext) for ext in EXCLUDE_FILES):
                continue

            file_path = os.path.join(root, file)
            if not any(file.endswith(ext) for ext in extensions):
                continue

            module_name = get_closest_module(file_path)

            if module_name:
                if module_name not in module_contents:
                    module_contents[module_name] = ""

                try:
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as infile:
                        code_content = infile.read()
                        indented_content = "\n".join("    " + line for line in code_content.splitlines())
                        content_block = f"\n# File: {file_path}\n\n```{file.split('.')[-1]}\n{indented_content}\n```\n"
                        module_contents[module_name] += content_block

                except Exception as e:
                    print(f"Error reading {file_path}: {e}")

    # Write each module's content to a separate file
    for module_name, content_str in module_contents.items():
        if content_str:  # Only create files for non-empty modules
            create_output_file(output_dir, module_name, project_structure, content_str)


# Example usage
concatenate_code_files("/home/matt/icn-prototype", max_depth=3)
