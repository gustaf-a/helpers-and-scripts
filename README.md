# helpers-and-scripts
Small scripts, helpers and tools

## Gustaf's Recursive Git Puller
Easily update all your git repositories in your source folder and subfolders. The recursive git puller script is a PowerShell script that provides an automated way to run a git pull command on every git repository under a given directory. It visually indicates success or failure for each repository and ensures you're up-to-date with the latest changes.

The user is provided colored visual feedback and can easily see if something goes wrong in any repository.

### How to Use
Clone this repo or download the script into the directory containing you repositories.
Execute the script. This will start the recursive git pull process beginning from the script's current location.

### How to create a shortcut to the script
1. Save your PowerShell script as myscript.ps1.

2. Create a new shortcut.

3. Set the shortcut target as:
C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe -ExecutionPolicy Bypass -File "[PathToScriptFile]"

4. Double-click the shortcut to run your script.
