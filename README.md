# helpers-and-scripts
Small scripts, helpers and tools

## Gustaf's Recursive Git Puller

This PowerShell script provides an automated way to run a git pull command on every git repository under a given directory. It visually indicates success or failure for each repository and ensures you're up-to-date with the latest changes in your current branch and the develop branch.

Features:

- The script provides colored visual feedback, making it easy to see if something goes wrong in any repository.
- Before running, the script asks for permission from the user to proceed.
- If the user is not on the develop branch, both the current branch and develop will be updated.
- If uncommitted changes are found, the script will ask the user to either skip the repo or stage and stash the changes before updating.
- The script can be run from any directory, and it will recursively search for git repositories in all subdirectories.
- The script allows the user to cancel the operation before it starts.

### How to Use
Clone this repo or download the script into the directory containing you repositories.
Execute the script. This will start the recursive git pull process beginning from the script's current location.

### How to create a shortcut to the script (Windows)
1. Save your PowerShell script as myscript.ps1.

2. Create a new shortcut.

3. Set the shortcut target as:
C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe -ExecutionPolicy Bypass -File "[PathToScriptFile]"

4. Double-click the shortcut to run your script.

### How to run script automatically on logon (Windows)
Make sure you never forget to update your git repos again.

0. Put the script in the repo folder or it's parent, or set the path in the script
1. Open Task Scheduler, click "Create Task" in the right pane.
2. Name the task, e.g., "Run GustafsGitPuller on Logon".
3. In the "General" tab, select "Run with highest privileges".
4. Go to "Triggers", click "New", and select "At log on" for your user.
5. In "Actions", click "New", choose "Start a program" and enter powershell.
6. For arguments, input -ExecutionPolicy Bypass -File "path\to\GustafsGitPuller.ps1".
7. In "Conditions", adjust settings if needed.
8. In "Settings", check "Allow task to be run on demand".
9. Save the task and provide any requested permissions.
