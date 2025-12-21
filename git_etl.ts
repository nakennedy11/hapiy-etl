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

      // grab date from either the author or committer field if it exists
      if (item.commit.author.date) {
        commitDate = new Date(item.commit.author.date);
      } else if (item.commit.committer && item.commit.committer.date) {
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
 * Reads and validates optional input from the config.json file
 *
 * @returns a RunOptions object containing either the valid options from config.json and/or the default values
 */
function readConfig(): RunOptions {
  // set default values of run options
  // will get replaced if there is valid config file inputs
  let repo: string = "cs4550hw01";
  let owner: string = "nakennedy11";
  let cronSchedule: string = "* * * * *";
  let kvPath: string = ".commitHistory.sqlite";
  let clearKvOnStartup: string = "Y";
  let useGithubToken: string = "N";

  // must have both repo/owner specified or both will use defaults
  if (configFile.owner && configFile.owner != "" && configFile.repo && configFile.repo != "") {
    owner = configFile.owner;
    repo = configFile.repo;
  }

  // cron schedule, validate able to parse
  if (configFile.cronSchedule && configFile.cronSchedule != "") {
    try {
      CronExpressionParser.parse(configFile.cronSchedule);
      cronSchedule = configFile.cronSchedule;
    } catch (err) {
      if (err instanceof Error) {
        console.log("Error parsing cron expression, using default of 5 minutes:", err.message);
      }
    }
  }

  // filepath validation? should end in sqllite
  if (configFile.kvPath && configFile.kvPath != "") {
    try {
      path.parse(configFile.kvPath);
      if (path.extname(configFile.kvPath) == ".sqlite") {
        kvPath = configFile.kvPath;
      } else {
        console.log("Incorrect file extension for kvPath, using default of .commitHistory.sqlite");
      }
    } catch (err) {
      if (err instanceof Error) {
        console.log("Error parsing kvPath, using default of .commitHistory.sqlite:", err.message);
      }
    }
  }

  // clearKvOnStartup should be Y or N
  if (configFile.clearKvOnStartup) {
    if (["Y", "N"].includes(configFile.clearKvOnStartup)) {
      clearKvOnStartup = configFile.clearKvOnStartup;
    } else {
      console.log("clearKvOnStartup must be Y or N. Using default of N");
    }
  }

  // useGithubToken should be Y or N
  if (configFile.useGithubToken) {
    if (["Y", "N"].includes(configFile.useGithubToken)) {
      useGithubToken = configFile.useGithubToken;
    } else {
      console.log("useGithubToken must be Y or N. Using default of N");
    }
  }

  return {
    repo: repo,
    owner: owner,
    cronSchedule: cronSchedule,
    kvPath: kvPath,
    clearKvOnStartup: clearKvOnStartup,
    useGithubToken: useGithubToken,
  };
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
