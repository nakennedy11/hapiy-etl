# Hapiy-ETL

## Overview
This project syncs a GitHub repositories commit history and stores it in a local database (Deno KV). On the first run of the program for a specific repository, the full history will be synced and stored, and subsequent runs will pull commit history incrementally after the maximum timestamp that has already been stored. The process will run continuously based on a Cron schedule, defaulted to every 5 minutes. The kv store will persist between runs, but can optionally be cleared on startup of the program to start with no history stored.

## Files 

| File | Description |
| - | - |
| git_etl.ts | Main program file. Used to run process. Contains logic to run loop of querying and storing git repo commit information. |
| config.json | Holds run configuration options, described below. File must be present to run.|

## Config
The require file config.json allows for certain options to be customized for the run of the program. Each option can be present/not present, and have restrictions on their format for proper usage.

| Option | Type | Description |
| ----------- | ----------- | ----------- |
| repo | string |The GitHub repository from which to read commit history. Must be used in conjunction with the owner option. |
| owner | string | The owner of the above GitHub repository. Must be used in conjunction with the repo option. |
| cronSchedule | string | The Cron expression that determines how often the program will check for new commit information. Must be a valid, parseable cron expression. |
| kvFilename | string | The name of the sqlite file in which the commit data is stored. Must end in .sqlite |
| clearKvOnStartup | boolean | Option to delete the sqlite files that match the prefix of a given or default kvFilename option when first running the program. Will delete all matching .sqlite files in the directory. |
| useGithubToken | boolean | **Highly recommended to avoid rate-limiting.** Option to use a github authentication token stored in the environment variable "GITHUB_PAT" when connecting to a repo. |

## How to run 
Project should be run from the same directory as git_etl.ts and config.json, and any existing sqlite Deno KV files. 
Run the command "deno install" in the project directory to install necessary dependencies. 
Fill in any wanted run config options in config.json
Run the command "deno run -A git_etl.ts" to run the project. 
