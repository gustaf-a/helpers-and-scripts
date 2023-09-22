param (
    [string]$sourceDirectory
)

function DisplayBanner {
    Clear-Host
    Write-Host @"
   ___         _         __ _       ___ _ _     ___      _ _         
  / __|_  _ __| |_ __ _ / _( )___  / __(_) |_  | _ \_  _| | |___ _ _ 
 | (_ | || (_-<  _/ _` |  _|/(_-< | (_ | |  _| |  _/ || | | / -_) '_|
  \___|\_,_/__/\__\__,_|_|   /__/  \___|_|\__| |_|  \_,_|_|_\___|_|  
                                                                     

Gustaf's Recursive Git Puller for quick repo updates
"@
    Write-Host "===============================" -ForegroundColor Cyan
}

function PullGitUpdates {
    Write-Host "Pulling updates..." -ForegroundColor Magenta

    $result = git pull --recurse-submodules

    if ($LASTEXITCODE -eq 0) {
        Write-Host "Pull successful!" -ForegroundColor Green
    } else {
        Write-Host "Pull failed!" -ForegroundColor Red
        Write-Host $result
    }
}

function RecursiveGitPull {
    param ([string]$path)

    Write-Host "-----------------------------------" -ForegroundColor Green

    Write-Host "Processing folder: $path" -ForegroundColor Yellow

    # Check if folder is a git repo
    if (Test-Path -Path (Join-Path $path ".git")) {
        Set-Location -Path $path

        # Check the current git branch
        $currentBranch = git rev-parse --abbrev-ref HEAD
        
        # Checks if the output of git status --porcelain is an empty string
        $hasUncommittedChanges = -not [string]::IsNullOrWhiteSpace((git status --porcelain))
        if ($hasUncommittedChanges){
            
            # Ask user what to do
            Write-Host "You have uncommitted changes" -ForegroundColor Yellow
            $userChoice = Read-Host " Input 1 to stage, stash and then update. Input 2 or press enter to skip this repo"
            if ($userChoice -ne "1") {
                return
            }

            # Stage all
            git add .
            if ($LASTEXITCODE -ne 0) {
                Write-Host "Staging failed: Skipping this repo" -ForegroundColor Red
                return
            }

            # Stash any uncommitted changes
            $stashResult = git stash
            if ($LASTEXITCODE -ne 0) {
                Write-Host "Stash failed: Skipping this repo" -ForegroundColor Red
                Write-Host $stashResult
                return
            }

            Write-Host "Stash successful" -ForegroundColor Green
        }

        PullGitUpdates

        # If it's not 'develop' switch to 'develop'
        if ($currentBranch -ne "develop") {
            # Notify the user about the current branch
            Write-Host "Current branch: $currentBranch" -ForegroundColor Magenta

            Write-Host "Switching to 'develop' branch to pull updates..." -ForegroundColor Magenta
            git checkout develop

            PullGitUpdates

            Write-Host "Switching back to $currentBranch branch..." -ForegroundColor Magenta
            git checkout $currentBranch
        }
        
        if ($hasUncommittedChanges){
            # Pop the stashed changes
            $popResult = git stash pop
            if ($LASTEXITCODE -ne 0) {
                Write-Host "Stash pop failed: Failed to restore uncommitted changes" -ForegroundColor Red
                Write-Host $popResult
            }
            else {
                Write-Host "Stash restored successful" -ForegroundColor Green
            }
        }

        return
    }
    else {
        # If not a git repo, look for subfolders and recursively call the function
        $subfolders = Get-ChildItem -Path $path -Directory
        foreach ($subfolder in $subfolders) {
            RecursiveGitPull -path $subfolder.FullName
        }
    }
}

function AllowUserToCancel {
    param (
        [string]$promptMessage = "Press Enter to run the script or Ctrl+C to exit."
        )

    Write-Host $promptMessage -ForegroundColor Yellow

    $input = Read-Host

    return $input
}

#Start
DisplayBanner

if ([string]::IsNullOrEmpty($sourceDirectory)) {
    #Assign file location as starting point
    $sourceDirectory = $PSScriptRoot

    #Assign default value here
    #$sourceDirectory = "path\to\repos\parent\folder"
}

Write-Host "Starting folder: $sourceDirectory" -ForegroundColor Yellow

AllowUserToCancel

Write-Host "===============================" -ForegroundColor Cyan

RecursiveGitPull -path $sourceDirectory

# Sign off message with separator
Write-Host "-----------------------------------" -ForegroundColor Green
Write-Host "Script execution complete." -ForegroundColor Cyan

# Keep window from closing
Read-Host -Prompt "Press Enter to exit"
