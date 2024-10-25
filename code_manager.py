import os
import re
from typing import Dict, List, Optional, Tuple
import difflib

class CodeManager:
    """
    Manages code updates based on specially formatted input files.
    Input files should have a header in this format:
    
    ```
    # TARGET: path/to/file.py
    # MODE: [replace|update|append]
    # SECTION: [optional] function or class name
    # DESCRIPTION: What this change does
    ```
    """
    
    def __init__(self, project_root: str):
        self.project_root = project_root
        self.backup_dir = os.path.join(project_root, '.code_backups')
        os.makedirs(self.backup_dir, exist_ok=True)

    def process_file(self, input_file: str, dry_run: bool = True) -> bool:
        """Process a code update file."""
        try:
            # Read and parse the input file
            with open(input_file, 'r') as f:
                content = f.read()

            # Parse header
            header = self._parse_header(content)
            if not header:
                print(f"Invalid header in {input_file}")
                return False

            # Get the code content
            code = self._extract_code(content)
            if not code:
                print(f"No code found in {input_file}")
                return False

            # Get target file path
            target_file = os.path.join(self.project_root, header['target'])
            if not os.path.exists(target_file):
                print(f"Target file not found: {target_file}")
                return False

            # Create backup
            if not dry_run:
                self._backup_file(target_file)

            # Process based on mode
            if header['mode'] == 'replace':
                success = self._replace_code(target_file, code, dry_run)
            elif header['mode'] == 'update':
                success = self._update_code(target_file, code, header.get('section'), dry_run)
            elif header['mode'] == 'append':
                success = self._append_code(target_file, code, dry_run)
            else:
                print(f"Unknown mode: {header['mode']}")
                return False

            if success and not dry_run:
                print(f"Successfully updated {target_file}")
            elif success:
                print(f"Dry run successful for {target_file}")
            return success

        except Exception as e:
            print(f"Error processing {input_file}: {str(e)}")
            return False

    def _parse_header(self, content: str) -> Optional[Dict[str, str]]:
        """Parse the header section of the input file."""
        header_pattern = r"#\s*TARGET:\s*(.+)\n#\s*MODE:\s*(.+)\n(?:#\s*SECTION:\s*(.+)\n)?#\s*DESCRIPTION:\s*(.+)"
        match = re.match(header_pattern, content)
        if not match:
            return None

        return {
            'target': match.group(1).strip(),
            'mode': match.group(2).strip().lower(),
            'section': match.group(3).strip() if match.group(3) else None,
            'description': match.group(4).strip()
        }

    def _extract_code(self, content: str) -> Optional[str]:
        """Extract the code section after the header."""
        lines = content.split('\n')
        start_idx = 0
        for i, line in enumerate(lines):
            if not line.strip().startswith('#'):
                start_idx = i
                break
        return '\n'.join(lines[start_idx:]).strip()

    def _backup_file(self, filepath: str) -> None:
        """Create a backup of the target file."""
        import time
        import shutil
        
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        backup_name = f"{os.path.basename(filepath)}.{timestamp}.bak"
        backup_path = os.path.join(self.backup_dir, backup_name)
        
        shutil.copy2(filepath, backup_path)

    def _replace_code(self, target_file: str, new_code: str, dry_run: bool) -> bool:
        """Replace entire file content."""
        if dry_run:
            with open(target_file, 'r') as f:
                old_code = f.read()
            self._show_diff(old_code, new_code)
            return True

        try:
            with open(target_file, 'w') as f:
                f.write(new_code)
            return True
        except Exception as e:
            print(f"Error replacing code: {str(e)}")
            return False

    def _update_code(self, target_file: str, new_code: str, section: Optional[str], dry_run: bool) -> bool:
        """Update specific section of code."""
        try:
            with open(target_file, 'r') as f:
                content = f.read()

            if section:
                # Find the section (class or function)
                section_pattern = rf"(class|def)\s+{section}\b[^\n]*:"
                match = re.search(section_pattern, content)
                if not match:
                    print(f"Section '{section}' not found")
                    return False

                # Find section bounds
                start = match.start()
                end = self._find_section_end(content, start)
                
                updated_content = content[:start] + new_code + content[end:]
            else:
                updated_content = new_code

            if dry_run:
                self._show_diff(content, updated_content)
                return True

            with open(target_file, 'w') as f:
                f.write(updated_content)
            return True

        except Exception as e:
            print(f"Error updating code: {str(e)}")
            return False

    def _append_code(self, target_file: str, new_code: str, dry_run: bool) -> bool:
        """Append code to end of file."""
        try:
            with open(target_file, 'r') as f:
                content = f.read()

            updated_content = content.rstrip() + '\n\n' + new_code + '\n'

            if dry_run:
                self._show_diff(content, updated_content)
                return True

            with open(target_file, 'w') as f:
                f.write(updated_content)
            return True

        except Exception as e:
            print(f"Error appending code: {str(e)}")
            return False

    def _find_section_end(self, content: str, start: int) -> int:
        """Find the end of a code section based on indentation."""
        lines = content[start:].split('\n')
        if not lines:
            return start

        # Get section indentation level
        first_line = lines[0]
        section_indent = len(first_line) - len(first_line.lstrip())
        
        # Find where indentation returns to original level
        current_pos = start + len(first_line) + 1
        for line in lines[1:]:
            if line.strip() and len(line) - len(line.lstrip()) <= section_indent:
                return current_pos
            current_pos += len(line) + 1

        return len(content)

    def _show_diff(self, old_code: str, new_code: str) -> None:
        """Show a diff of the changes."""
        diff = difflib.unified_diff(
            old_code.splitlines(keepends=True),
            new_code.splitlines(keepends=True),
            fromfile='old',
            tofile='new'
        )
        print(''.join(diff))

def main():
    """Command line interface for the code manager."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Manage code updates in the project")
    parser.add_argument('input_file', help="File containing the code update")
    parser.add_argument('--project-root', default='.', help="Root directory of the project")
    parser.add_argument('--dry-run', action='store_true', help="Show changes without applying them")
    
    args = parser.parse_args()
    
    manager = CodeManager(args.project_root)
    success = manager.process_file(args.input_file, args.dry_run)
    
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())
