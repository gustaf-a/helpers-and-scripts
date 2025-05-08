#!/usr/bin/env python3
import os
import sys
import shutil
import fnmatch
import argparse
import datetime
import subprocess
from dotenv import load_dotenv

# DEFAULT VALUES - Easy to modify
DEFAULT_VM_DEPLOY_PATH = "/home/AIDocument-user-dev/LLamaIndexLab/" #Use pwd in ssh to get the current directory
DEFAULT_SOURCE_DIR = "."
DEFAULT_DEPLOY_DIR = "../_temp_deploy_folder" 
DEFAULT_ENV_FILE = ".env_prod"
DEFAULT_IGNORE_PATTERNS = [".vscode", ".venv", "__pycache__", ".pytest_cache", ".git", "deploy.py", "deploy_instructions.md", "_temp_deploy_folder"]
DEFAULT_DOCKER_COMPOSE_DIR = "src"  # Default directory containing docker-compose.yml

try:
    from tqdm import tqdm
except ImportError:
    print("Please install tqdm (pip install tqdm) for progress bars.")
    sys.exit(1)

# Load environment variables from .env file
def load_env_variables():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
    
    # Get database config from environment variables
    default_host = os.environ.get("db__host")
    default_port = os.environ.get("db__port")
    
    return default_host, default_port

default_host, default_port = load_env_variables()

def gather_files(src_root, ignore_patterns):
    """Walk src_root and return list of relative file-paths, skipping ignores."""
    files = []
    for root, dirs, filenames in os.walk(src_root):
        # filter out ignored directories in-place
        dirs[:] = [d for d in dirs 
                   if not any(fnmatch.fnmatch(d, pat) for pat in ignore_patterns)]
        for fn in filenames:
            if any(fnmatch.fnmatch(fn, pat) for pat in ignore_patterns):
                continue
            full = os.path.join(root, fn)
            rel = os.path.relpath(full, src_root)
            files.append(rel)
    return files

def copy_with_progress(src_root, dst_root, ignore_patterns):
    files = gather_files(src_root, ignore_patterns)
    for rel in tqdm(files, desc="Copying", unit="files"):
        src_path = os.path.join(src_root, rel)
        dst_path = os.path.join(dst_root, rel)
        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        shutil.copy2(src_path, dst_path)

def run_scp(key_path, local_path, vm_ip, remote_path):
    print("\nCreating remote directory if needed...")
    ssh_cmd = [
        "ssh",
        "-i", key_path,
        vm_ip,
        f"mkdir -p {remote_path}"
    ]
    result = subprocess.run(ssh_cmd)
    if result.returncode != 0:
        print(f"SSH mkdir failed with exit code {result.returncode}")
        sys.exit(result.returncode)

    print("Starting transfer via scp...")
    # Get all items in the local_path directory
    items = os.listdir(local_path)
    
    for item in tqdm(items, desc="Transferring", unit="items"):
        item_path = os.path.join(local_path, item)
        scp_cmd = [
            "scp",
            "-i", key_path,
            "-r" if os.path.isdir(item_path) else "",
            item_path,
            f"{vm_ip}:{remote_path}"
        ]
        # Filter out empty strings (when not using -r flag for files)
        scp_cmd = [cmd for cmd in scp_cmd if cmd]
        
        result = subprocess.run(scp_cmd)
        if result.returncode != 0:
            print(f"SCP failed for {item} with exit code {result.returncode}")
            sys.exit(result.returncode)
    
    print("Transfer complete.")

def run_remote_command(key_path, vm_ip, command, description=None):
    """Execute a command on the remote server via SSH."""
    if description:
        print(f"\n{description}...")
    
    ssh_cmd = [
        "ssh",
        "-i", key_path,
        vm_ip,
        command
    ]
    result = subprocess.run(ssh_cmd)
    if result.returncode != 0:
        print(f"Remote command failed with exit code {result.returncode}")
        return False
    return True

def replace_env_files(deploy_dir, source_env_path):
    """Find all .env files in deploy_dir and replace them with source_env_path."""
    count = 0
    for root, _, files in os.walk(deploy_dir):
        for file in files:
            if file == ".env":
                target_path = os.path.join(root, file)
                shutil.copy2(source_env_path, target_path)
                count += 1
    return count

def remove_env_variant_files(deploy_dir):
    """Remove all .env_* files in the deploy_dir."""
    count = 0
    for root, _, files in os.walk(deploy_dir):
        for file in files:
            if file.startswith(".env_"):
                target_path = os.path.join(root, file)
                os.remove(target_path)
                count += 1
    return count

def main():
    parser = argparse.ArgumentParser(
        description="Package and deploy to a Linux VM via scp."
    )
    parser.add_argument("--key-path",     required=True, help="Path to your .pem SSH key")
    parser.add_argument("--vm-ip",        required=True, help="User@host or host (e.g. azureuser@1.2.3.4)")
    parser.add_argument("--vm-deploy-path", default=DEFAULT_VM_DEPLOY_PATH,
                        help=f"Remote directory into which to copy (default: {DEFAULT_VM_DEPLOY_PATH})")
    parser.add_argument("--source",       default=DEFAULT_SOURCE_DIR,
                        help=f"Local source folder (default: {DEFAULT_SOURCE_DIR})")
    parser.add_argument("--base-deploy-dir", default=DEFAULT_DEPLOY_DIR,
                        help=f"Base local deploy folder (default: {DEFAULT_DEPLOY_DIR})")
    parser.add_argument("--host", default=default_host, help="Host from environment (optional)")
    parser.add_argument("--port", default=default_port, help="Port from environment (optional)")
    parser.add_argument("--docker", action="store_true", 
                        help="Stop all containers, build, and start Docker containers on remote server")
    parser.add_argument("--dryrun", action="store_true",
                        help="Only create the deploy folder locally, don't copy to server")
    parser.add_argument("--env-file", default=DEFAULT_ENV_FILE,
                        help=f"Name of the environment file to use for Azure VM (default: {DEFAULT_ENV_FILE})")
    parser.add_argument("--docker-compose-dir", default=DEFAULT_DOCKER_COMPOSE_DIR,
                        help=f"Directory containing the docker-compose.yml file (default: {DEFAULT_DOCKER_COMPOSE_DIR})")
    args = parser.parse_args()

    # 1) Use the deploy directory directly without date subfolder
    deploy_dir = args.base_deploy_dir
    if os.path.exists(deploy_dir):
        print(f"Removing existing deploy directory: {deploy_dir}")
        shutil.rmtree(deploy_dir)
    os.makedirs(deploy_dir, exist_ok=True)

    # 2) copy with progress
    ignore = DEFAULT_IGNORE_PATTERNS
    print(f"Copying '{args.source}' → '{deploy_dir}' (ignoring {ignore})")
    copy_with_progress(args.source, deploy_dir, ignore)

    # Replace .env files with specified environment file
    env_file_path = os.path.join(args.source, args.env_file)
    if os.path.exists(env_file_path):
        print(f"Replacing all .env files with {args.env_file}")
        count = replace_env_files(deploy_dir, env_file_path)
        print(f"Replaced {count} .env file(s)")
    else:
        raise FileNotFoundError(f"{args.env_file} not found in {args.source}")
    
    # Remove all .env_* files
    removed_count = remove_env_variant_files(deploy_dir)
    print(f"Removed {removed_count} .env_* file(s)")

    # Exit if dryrun is enabled
    if args.dryrun:
        print(f"\nDry run completed. Files prepared in '{deploy_dir}' but not transferred to server.")
        return

    # 3) transfer via scp
    run_scp(args.key_path, deploy_dir, args.vm_ip, args.vm_deploy_path)
    
    # 4) Run Docker commands if requested
    if args.docker:
        docker_path = os.path.join(args.vm_deploy_path, args.docker_compose_dir)
        
        # Stop all running containers
        run_remote_command(
            args.key_path, 
            args.vm_ip, 
            "docker stop $(docker ps -q) || true",
            "Stopping all Docker containers"
        )
        
        # Build containers
        success = run_remote_command(
            args.key_path, 
            args.vm_ip, 
            f"cd {docker_path} && docker compose build",
            "Building Docker containers"
        )
        if not success:
            print("Docker build failed, stopping.")
            sys.exit(1)
        
        # Start containers
        success = run_remote_command(
            args.key_path, 
            args.vm_ip, 
            f"cd {docker_path} && docker compose up -d",
            "Starting Docker containers"
        )
        if not success:
            print("Docker start failed.")
            sys.exit(1)
        else:
            print("Docker containers started successfully.")

if __name__ == "__main__":
    main()
