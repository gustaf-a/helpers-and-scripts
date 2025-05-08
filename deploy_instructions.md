# Deployment Instructions

The `deploy.py` script is used to package and deploy the application to a Linux VM via SCP.

## Setup

1. Place the `deploy.py` script in the project root directory.
2. Ensure you have the necessary environment files (`.env_prod` by default).
3. Install required Python dependencies:
   ```bash
   pip install tqdm python-dotenv
   ```

## Usage

Basic usage:
```bash
python deploy.py --key-path /path/to/ssh_key.pem --vm-ip username@vm_ip_address
```

### Common Examples

Deploy with Docker operations:
```bash
python deploy.py --key-path ~/.ssh/mykey.pem --vm-ip azureuser@10.0.0.4 --docker
```

Deploy with custom environment file:
```bash
python deploy.py --key-path ~/.ssh/mykey.pem --vm-ip azureuser@10.0.0.4 --env-file .env_staging
```

Perform a dry run (prepare files without deploying):
```bash
python deploy.py --key-path ~/.ssh/mykey.pem --vm-ip azureuser@10.0.0.4 --dryrun
```

### Available Options

- `--key-path`: Path to your SSH key (required)
- `--vm-ip`: VM IP or hostname with username (required)
- `--vm-deploy-path`: Remote directory path (default: `/home/AIDocument-user-dev/LLamaIndexLab/`)
- `--source`: Local source folder (default: current directory)
- `--base-deploy-dir`: Temporary deploy folder (default: `../_temp_deploy_folder`)
- `--env-file`: Environment file to use (default: `.env_prod`)
- `--docker`: Build and start Docker containers after deployment
- `--docker-compose-dir`: Directory containing the docker-compose.yml (default: `src`)
- `--dryrun`: Prepare files locally without deploying