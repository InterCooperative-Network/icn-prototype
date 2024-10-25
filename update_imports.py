import os

def update_imports(directory):
    """Update all imports of types to shard_types in Python files."""
    replacements = [
        ('from .shard_types import', 'from .shard_types import'),
        ('from blockchain.core.shard.shard_types import', 'from blockchain.core.shard.shard_types import'),
        ('from ..core.shard.shard_types import', 'from ..core.shard.shard_types import')
    ]
    
    # Walk through all Python files
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                filepath = os.path.join(root, file)
                try:
                    # Read file content
                    with open(filepath, 'r') as f:
                        content = f.read()
                    
                    # Check if any replacements are needed
                    original_content = content
                    for old, new in replacements:
                        if old in content:
                            content = content.replace(old, new)
                    
                    # Only write if changes were made
                    if content != original_content:
                        print(f"Updating imports in {filepath}")
                        with open(filepath, 'w') as f:
                            f.write(content)
                
                except Exception as e:
                    print(f"Error processing {filepath}: {e}")

if __name__ == "__main__":
    # Run from project root directory
    update_imports(".")
    print("Import updates completed!")
