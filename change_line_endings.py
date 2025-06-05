import os

# folder_path = "C:/path/to/folder"
folder_path = "C:/PrivateRepos/Nexer/ComputerUseQueueAdapter"

for root, dirs, files in os.walk(folder_path):
    # Skip directories starting with `.`
    dirs[:] = [d for d in dirs if not d.startswith('.')]
    for file in files:
        file_path = os.path.join(root, file)
        try:
            # Try reading the file with UTF-8
            with open(file_path, 'r', encoding='utf-8', newline='') as f:
                content = f.read()
        except UnicodeDecodeError:
            print(f"Skipping {file_path}: Cannot decode file with UTF-8.")
            continue

        # Replace Windows line endings with Unix line endings
        content = content.replace('\r\n', '\n')

        try:
            # Write the updated content back to the file
            with open(file_path, 'w', encoding='utf-8', newline='') as f:
                f.write(content)
        except Exception as e:
            print(f"Error writing to {file_path}: {e}")

print("Line endings converted to Unix format, skipping problematic files.")