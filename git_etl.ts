// git api
import { Octokit } from "npm:octokit";
import { Endpoints } from "@octokit/types";
import { SECOND } from "@std/datetime";
import { CronExpressionParser } from "npm:cron-parser";
import * as path from "@std/path";

import configFile from "./config.json" with { type: "json" };

type ListCommitsResponse = Endpoints["GET /repos/{owner}/{repo}/commits"]["response"]["data"];

/**
 * Contains data from git commits to be stored
 */
type CommitData = {
  commitHash: string;
  commitTimestamp: Date | undefined;
  commitMessage: string;
  authorEmail: string | undefined;
};

/**
 * Contains data for the various conifgurations that can be used for running the application
 */
type RunOptions = {
  repo: string;
  owner: string;
  cronSchedule: string;
  kvPath: string;
  clearKvOnStartup: string;
  useGithubToken: string;
};

/**
 * Contains the repo and owner information for a GitHub repository
 */
type RepoInfo = {
  repo: string;
  owner: string;
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
  const data = await octokit.paginate("GET /repos/{owner}/{repo}/commits", {
    owner: owner,
    repo: repo,
    since: timestamp?.toISOString(),
  });

  if (timestamp) {
    console.log(`${data.length} new commits found after timestamp ${timestamp}`);
  } else {
    console.log(`${data.length} new commits found for initial historical load`);
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
    let commit: CommitData | undefined;
    let authorEmail: string | undefined;
    let commitDate: Date | undefined;

    if (item.commit.author) {
      authorEmail = item.commit.author.email;

      // pull date from either the author field if it exists
      if (item.commit.author.date) {
        commitDate = new Date(item.commit.author.date);
      }
    }

    // try to pull date from committer field if not assigned from author field
    if (!commitDate) {
      if (item.commit.committer && item.commit.committer.date) {
        commitDate = new Date(item.commit.committer.date);
      }
    }

    commit = {
      commitHash: item.sha,
      commitTimestamp: commitDate,
      commitMessage: item.commit.message,
      authorEmail: authorEmail,
    };

    if (commit) {
      parsedCommits.push(commit);
    }
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
async function getLatestCommitTimestamp(kv: Deno.Kv, repo: string): Promise<Date | undefined> {
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
 * Validates the "repo" and "owner" config file options. Each must be present and valid for either to be used.
 *
 * @param owner - the value of the "owner" field from the config file
 * @param defaultOwner - the default value to be used if "owner" is not present or valid
 * @param repo - the value of the "repo" field from the config file
 * @param defaultRepo - the default value to be used if the "repo" field is not present or valid
 * @returns a RepoInfo object containing the repo and owner strings
 */
function validateRepoConfig(owner: unknown, defaultOwner: string, repo: unknown, defaultRepo: string): RepoInfo {
  // must have both repo/owner specified or both will use defaults
  let repoInfo: RepoInfo;

  if (owner && typeof owner === "string" && owner != "" && repo && typeof repo === "string" && repo != "") {
    repoInfo = {
      repo: repo,
      owner: owner,
    };
  } else {
    repoInfo = {
      repo: defaultRepo,
      owner: defaultOwner,
    };
  }

  return repoInfo;
}

/**
 * Validates the "cronSchedule" config file option. Must be a string and parse-able as a Cron expression
 *
 * @param cronExp - the value of the "cronSchedule" field from the config file
 * @param defaultCronExp - the default value to be used if "cronSchedule" is not present or is invalid
 * @returns either the validated config file value or default value of a Cron expression as a string
 */
function validateCronConfig(cronExp: unknown, defaultCronExp: string): string {
  // cron schedule, validate able to parse
  if (cronExp && typeof cronExp === "string" && cronExp != "") {
    try {
      CronExpressionParser.parse(configFile.cronSchedule);
      return cronExp;
    } catch (err) {
      if (err instanceof Error) {
        console.log("Error parsing cron expression, using default of 5 minutes:", err.message);
      }
    }
  }

  return defaultCronExp;
}

/**
 * Validates the "kvPath" config file option. Must be a string, parseable as a path, ending in .sqlite
 *
 * @param kvPath - the value of the "kvPath" field from the config file
 * @param defaultKvPath - the default value to be used if kvPath does not exist or is invalid
 * @returns either the validated kvPath config value or the passed default kvPath as a string
 */
function validateKvPathConfig(kvPath: unknown, defaultKvPath: string): string {
  // filepath validation, should end in sqllite
  if (kvPath && typeof kvPath === "string" && kvPath != "") {
    try {
      path.parse(configFile.kvPath);
      if (path.extname(configFile.kvPath) == ".sqlite") {
        return kvPath;
      } else {
        console.log("Incorrect file extension for kvPath, using default of .commitHistory.sqlite");
      }
    } catch (err) {
      if (err instanceof Error) {
        console.log("Error parsing kvPath, using default of .commitHistory.sqlite:", err.message);
      }
    }
  }

  return defaultKvPath;
}

/**
 * Validates a config file option that can only be "Y" or "N" (i.e., clearKvOnStartup or useGithubToken)
 *
 * @param fieldName - the name of the config file field being validated
 * @param defaultVal - the default value of either "Y" or "N" to be use if configVal is invalid
 * @param configVal - the value of the given config file field
 * @returns either the validated "Y" or "N" option from the config file or the passed default value
 */
function validateYNConfig(fieldName: string, defaultVal: string, configVal: unknown): string {
  if (configVal && typeof configVal === "string" && ["Y", "N"].includes(configVal)) {
    return configVal;
  } else {
    console.log(`${fieldName} must be Y or N. Using default of ${defaultVal}`);
    return defaultVal;
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
    kvPath: ".commitHistory.sqlite",
    clearKvOnStartup: "Y",
    useGithubToken: "N",
  };

  const runOptions = {
    ...validateRepoConfig(configFile.owner, defaultRunOptions.owner, configFile.repo, defaultRunOptions.repo),
    cronSchedule: validateCronConfig(configFile.cronSchedule, defaultRunOptions.cronSchedule),
    kvPath: validateKvPathConfig(configFile.kvPath, defaultRunOptions.kvPath),
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
 * @param kvPath - the path of kv store files to be potentially removed
 */
async function clearKvFiles(kvPath: string): Promise<void> {
  console.log("Cleaning kv directory");
  try {
    // verify directory exists
    await Deno.lstat(kvPath);

    for await (const entry of Deno.readDir("./")) {
      if (entry.isFile && entry.name.includes(".sqlite")) {
        console.log(`Removing file: ${entry.name}`);
        Deno.remove(`./${entry.name}`);
      }
    }
  } catch (err) {
    if (!(err instanceof Deno.errors.NotFound)) {
      throw err;
    }
    console.log("Specified directory does not exist, nothing to clean up");
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
  let maxTs = await getLatestCommitTimestamp(commitKv, repo);

  if (maxTs) {
    // increment maximum commit timestamp by 1 second to get only new commits
    maxTs = new Date(maxTs.getTime() + SECOND);
  }

  const data = await getCommits(octokit, repo, owner, maxTs);

  const parsedCommits: CommitData[] = parseCommits(data);
  console.log(parsedCommits);

  await storeCommits(commitKv, repo, parsedCommits);
}

if (import.meta.main) {
  const options: RunOptions = readConfig();

  if (options.clearKvOnStartup == "Y") {
    await clearKvFiles(options.kvPath);
  }

  let token: string | undefined;

  if (options.useGithubToken == "Y") {
    token = Deno.env.get("GITHUB_PAT");
  }

  const octokit: Octokit = new Octokit({ auth: token });

  const commitKv = await Deno.openKv(options.kvPath);

  Deno.cron("Run loop to get and store commit history", options.cronSchedule, async () => {
    await main(commitKv, octokit, options.repo, options.owner);
  });
}
