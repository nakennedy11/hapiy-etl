// git api
import { Octokit } from "octokit";
import { Endpoints } from "@octokit/types";
import { SECOND } from "@std/datetime";
import { CronExpressionParser } from "cron-parser";
import * as path from "@std/path";

import configFile from "./config.json" with { type: "json" };

const ListCommitsEndpoint = "GET /repos/{owner}/{repo}/commits" as const;
type ListCommitsResponse = Endpoints["GET /repos/{owner}/{repo}/commits"]["response"]["data"];
const GithubTokenEnvironmentVariableName = "GITHUB_PAT" as const;

/**
 * Contains data from git commits to be stored
 */
type CommitData = {
  commitHash: string;
  commitTimestamp: Date | undefined;
  commitMessage: string;
  commitEmail: string | undefined;
};

/**
 * Contains the repo and owner information for a GitHub repository
 */
type RepoInfo = {
  repo: string;
  owner: string;
};

/**
 * Contains data for the various conifgurations that can be used for running the application
 */
type RunOptions =
  & RepoInfo
  & {
    cronSchedule: string;
    kvFilename: string;
    clearKvOnStartup: boolean;
    useGithubToken: boolean;
  };

/**
 * Retrieves a list of git commits from a specified GitHub repository, either historically or after a given timestamp
 *
 * @param octokit - the Octokit client used to make requests to the Github API
 * @param repo - name of the GitHub repository to read commit information from
 * @param owner - owner of the GitHub repository
 * @param timestamp - optional - the minimum timestamp after which commits should be read
 * @returns a ListCommitsResponse, an array of git commit information from the specified repo
 */
async function getCommits(
  octokit: Octokit,
  repo: string,
  owner: string,
  timestamp?: Date,
): Promise<ListCommitsResponse> {
  // use paginate to handle larger repo commit history datasets
  const data = await octokit.paginate(ListCommitsEndpoint, {
    owner: owner,
    repo: repo,
    since: timestamp?.toISOString(),
    per_page: 100,
  });

  if (timestamp) {
    console.info(`${data.length} new commits found after timestamp ${timestamp}`);
  } else {
    console.info(`${data.length} new commits found for initial historical load`);
  }

  return data;
}

/**
 * Parses commit information returned by the GitHub API into the needed CommitData for storage
 *
 * @param commits - the List of commit information returned from the GitHub API
 * @returns a list of CommitData objects containing limited and formatted information from the commits list
 */
function parseCommits(commits: ListCommitsResponse): CommitData[] {
  const parsedCommits: CommitData[] = [];

  for (const item of commits) {
    //let commit: CommitData | undefined;
    let commitEmail: string | undefined;
    let commitDate: Date | undefined;

    if (item.commit.author) {
      commitEmail = item.commit.author.email;

      // pull date from either the author field if it exists
      if (item.commit.author.date) {
        commitDate = new Date(item.commit.author.date);
      }
    }

    // try to pull values from committer field if date was not assigned from author field
    if (!commitDate) {
      if (item.commit.committer && item.commit.committer.date) {
        commitEmail = item.commit.committer.email;
        commitDate = new Date(item.commit.committer.date);
      }
    }

    const commit = {
      commitHash: item.sha,
      commitTimestamp: commitDate,
      commitMessage: item.commit.message,
      commitEmail: commitEmail,
    };

    parsedCommits.push(commit);
  }

  return parsedCommits;
}

/**
 * Puts a list of CommitData objects into the given Deno KV store
 *
 * @param kv - the Deno KV instance to store commit information in
 * @param repo - the name of the repo where the commits are from, used to separate commits by source in the KV instance
 * @param commits - the list of commits to be stored in the given KV instance
 */
async function storeCommits(kv: Deno.Kv, repo: string, commits: CommitData[]): Promise<void> {
  for (const commit of commits) {
    const key = commit.commitHash;
    await kv.set([`commits/${repo}`, key], commit);
  }
}

/**
 * Retrieves the maximum timestamp of all commits from the given repo in the given KV instance
 *
 * @param kv - the Deno KV instance to search for timestamps
 * @param repo - the source GitHub repository used to find commits under their specific prefix
 * @returns a Date object of the maximum timestamp if found, else undefined if no max timestamp exists
 */
async function getLatestStoredCommitTimestamp(kv: Deno.Kv, repo: string): Promise<Date | undefined> {
  const commitIter = kv.list<CommitData>({ prefix: [`commits/${repo}`] });
  let maxTimestamp: Date | undefined;

  for await (const commit of commitIter) {
    const timestamp: Date | undefined = commit.value.commitTimestamp;
    if (!maxTimestamp || (timestamp && timestamp > maxTimestamp)) {
      maxTimestamp = timestamp;
    }
  }

  return maxTimestamp;
}

/**
 * Identifies an unknown object as a string and confirms it is a non-empty value
 *
 * @param cfg - the object to validate as a non-empty string type
 * @returns true if conditions for non-empty string are met, otherwise false
 */
function isNonEmptyConfigString(cfg: unknown): cfg is string {
  if (cfg && typeof cfg === "string" && cfg !== "") {
    return true;
  }

  return false;
}

/**
 * Validates the "repo" and "owner" config file options. Each must be present and valid for either to be used.
 *
 * @param owner - the value of the "owner" field from the config file
 * @param defaultOwner - the default value to be used if "owner" is not present or valid
 * @param repo - the value of the "repo" field from the config file
 * @param defaultRepo - the default value to be used if the "repo" field is not present or valid
 * @returns a RepoInfo object containing the repo and owner strings
 */
function validateRepoConfig(owner: unknown, defaultOwner: string, repo: unknown, defaultRepo: string): RepoInfo {
  const validOwner = isNonEmptyConfigString(owner);
  const validRepo = isNonEmptyConfigString(repo);

  if (validOwner && validRepo) {
    return {
      repo: repo,
      owner: owner,
    };
    // must have both repo/owner specified
  } else if ((validOwner && !validRepo) || (!validOwner && validRepo)) {
    throw new Error("Both owner and repo must be specified and valid strings to use in config");
  }

  return {
    repo: defaultRepo,
    owner: defaultOwner,
  };
}

/**
 * Validates the "cronSchedule" config file option. Must be a string and parse-able as a Cron expression
 *
 * @param cronExp - the value of the "cronSchedule" field from the config file
 * @param defaultCronExp - the default value to be used if "cronSchedule" is not present or is invalid
 * @returns either the validated config file value or default value of a Cron expression as a string
 */
function validateCronConfig(cronExpression: unknown, defaultCronExp: string): string {
  // cron schedule, validate able to parse
  if (isNonEmptyConfigString(cronExpression)) {
    CronExpressionParser.parse(configFile.cronSchedule);
    return cronExpression;
  }

  console.info(`Using default value for cronSchedule: ${defaultCronExp}`);
  return defaultCronExp;
}

/**
 * Validates the "kvFilename" config file option. Must be a string, parseable as a path, ending in .sqlite
 *
 * @param kvFilename - the value of the "kvFilename" field from the config file
 * @param defaultkvFilename - the default value to be used if kvFilename does not exist or is invalid
 * @returns either the validated kvFilename config value or the passed default kvFilename as a string
 */
function validatekvFilenameConfig(kvFilename: unknown, defaultkvFilename: string): string {
  // filepath validation, should end in sqllite
  if (isNonEmptyConfigString(kvFilename)) {
    path.parse(configFile.kvFilename);
    if (path.extname(configFile.kvFilename) === ".sqlite") {
      return kvFilename;
    } else {
      throw new Error("Incorrect file extension for kvFilename, must end in .sqlite");
    }
  }

  console.info(`Using default value for kvFilename: ${defaultkvFilename}`);
  return defaultkvFilename;
}

/**
 * Validates a config file option that can only be "Y" or "N" (i.e., clearKvOnStartup or useGithubToken)
 *
 * @param fieldName - the name of the config file field being validated
 * @param defaultVal - the default value of either "Y" or "N" to be use if configVal is invalid
 * @param configVal - the value of the given config file field
 * @returns either the validated "Y" or "N" option from the config file or the passed default value
 */
function validateYNConfig(fieldName: string, defaultVal: boolean, configVal: unknown): boolean {
  if (configVal === undefined) {
    console.info(`Using default value for ${fieldName}: ${defaultVal}`);
    return defaultVal;
  } else if (typeof configVal === "boolean") {
    return configVal;
  } else {
    throw new Error(`${fieldName} must be a boolean value of true or false`);
  }
}

/**
 * Reads and validates optional input from the config.json file
 *
 * @returns a RunOptions object containing either the valid options from config.json and/or the default values
 */
function readConfig(): RunOptions {
  // set default values of run options
  const defaultRunOptions = {
    repo: "cs4550hw01",
    owner: "nakennedy11",
    cronSchedule: "*/5 * * * *",
    kvFilename: ".commitHistory.sqlite",
    clearKvOnStartup: true,
    useGithubToken: false,
  };

  const runOptions = {
    ...validateRepoConfig(configFile.owner, defaultRunOptions.owner, configFile.repo, defaultRunOptions.repo),
    cronSchedule: validateCronConfig(configFile.cronSchedule, defaultRunOptions.cronSchedule),
    kvFilename: validatekvFilenameConfig(configFile.kvFilename, defaultRunOptions.kvFilename),
    clearKvOnStartup: validateYNConfig(
      "clearKvOnStartup",
      defaultRunOptions.clearKvOnStartup,
      configFile.clearKvOnStartup,
    ),
    useGithubToken: validateYNConfig("useGithubToken", defaultRunOptions.useGithubToken, configFile.useGithubToken),
  };

  return runOptions;
}

/**
 * Deletes all .sqlite files in a given directory if it exists to remove previous commit data
 *
 * @param kvFilename - the name of kv store files to be potentially removed
 */
async function clearKvFiles(kvFilename: string): Promise<void> {
  console.info("Cleaning kv directory");
  try {
    console.info(`Removing files with a prefix of ${kvFilename}`);

    for await (const entry of Deno.readDir("./")) {
      if (entry.isFile && entry.name.includes(kvFilename)) {
        console.info(`Removing file: ${entry.name}`);
        await Deno.remove(`./${entry.name}`);
      }
    }
  } catch (err) {
    console.error("Error deleting sqlite files");
    throw err;
  }
}

/**
 * Main function to orchestrate logic of finding historical/new commit information and storing it
 *
 * @param commitKv - the Deno KV instance to store commit information in
 * @param octokit - the Octokit instance to use for making requests to the GitHub API
 * @param repo - the GitHub repository to retrieve commit information from
 * @param owner - the owner of the GitHub repository
 */
async function main(commitKv: Deno.Kv, octokit: Octokit, repo: string, owner: string): Promise<void> {
  let maxTimestamp = await getLatestStoredCommitTimestamp(commitKv, repo);

  if (maxTimestamp) {
    // increment maximum commit timestamp by 1 second to get only new commits
    maxTimestamp = new Date(maxTimestamp.getTime() + SECOND);
  }

  const data = await getCommits(octokit, repo, owner, maxTimestamp);

  const parsedCommits: CommitData[] = parseCommits(data);

  await storeCommits(commitKv, repo, parsedCommits);
}

if (import.meta.main) {
  const options: RunOptions = readConfig();

  if (options.clearKvOnStartup) {
    await clearKvFiles(options.kvFilename);
  }

  let token: string | undefined;

  if (options.useGithubToken) {
    token = Deno.env.get(GithubTokenEnvironmentVariableName);
  }

  const octokit: Octokit = new Octokit({ auth: token });

  const commitKv = await Deno.openKv(options.kvFilename);

  // start initial run before waiting for the cron
  await main(commitKv, octokit, options.repo, options.owner);

  Deno.cron("Run loop to get and store commit history", options.cronSchedule, async () => {
    await main(commitKv, octokit, options.repo, options.owner);
  });
}
