// git api
import { Octokit } from "npm:octokit";
import { Endpoints } from "@octokit/types";
import { SECOND } from "@std/datetime";
import { CronExpressionParser } from "npm:cron-parser";
import * as path from "@std/path";

import configFile from "./config.json" with { type: "json" };

type ListCommitsResponse = Endpoints["GET /repos/{owner}/{repo}/commits"]["response"]["data"];

type CommitData = {
  commitHash: string;
  commitTimestamp: Date | undefined;
  commitMessage: string;
  authorEmail: string | undefined;
};

type RunOptions = {
  repo: string;
  owner: string;
  cronSchedule: string;
  kvPath: string;
  clearKvOnStartup: string;
  useGithubToken: string;
};

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

function parseCommits(commits: ListCommitsResponse): CommitData[] {
  const parsedCommits: CommitData[] = [];

  for (const item of commits) {
    let commit: CommitData | undefined = undefined;

    if (item.commit.author && item.commit.author.date) {
      commit = {
        commitHash: item.sha,
        commitTimestamp: new Date(item.commit.author.date),
        commitMessage: item.commit.message,
        authorEmail: item.commit.author.email,
      };
    } else {
      commit = {
        commitHash: item.sha,
        commitTimestamp: undefined,
        commitMessage: item.commit.message,
        authorEmail: undefined,
      };
    }

    if (commit) {
      parsedCommits.push(commit);
    }
  }

  return parsedCommits;
}

async function storeCommits(kv: Deno.Kv, repo: string, commits: CommitData[]): Promise<void> {
  for (const commit of commits) {
    const key = commit.commitHash;
    await kv.set([`commits/${repo}`, key], commit);
  }
}

async function getLatestCommitTimestamp(kv: Deno.Kv, repo: string): Promise<Date | undefined> {
  const commitIter = kv.list<CommitData>({ prefix: [`commits/${repo}`] });
  let maxTimestamp: Date | undefined = undefined;

  for await (const commit of commitIter) {
    const timestamp: Date | undefined = commit.value.commitTimestamp;
    if (!maxTimestamp || (timestamp && timestamp > maxTimestamp)) {
      maxTimestamp = timestamp;
    }
  }

  return maxTimestamp;
}

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
  if (configFile.clearKvOnStartup &&  ["Y", "N"].includes(configFile.clearKvOnStartup)) {
    clearKvOnStartup = configFile.clearKvOnStartup;
  }

  // useGithubToken should be Y or N
  if (configFile.useGithubToken && ["Y", "N"].includes(configFile.useGithubToken)) {
    useGithubToken = configFile.useGithubToken;
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

async function clearKvFiles(kvPath: string): Promise<void> {
  console.log("Cleaning kv directory");
  try {
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

async function main(commitKv: Deno.Kv, octokit: Octokit, repo: string, owner: string): Promise<void> {
  let maxTs = await getLatestCommitTimestamp(commitKv, repo);

  if (maxTs) {
    // increment maximum commit timestamp by 1 second to get only new commits
    maxTs = new Date(maxTs.getTime() + SECOND);
  }

  const data = await getCommits(octokit, repo, owner, maxTs);

  const testcommits: CommitData[] = parseCommits(data);
  console.log(testcommits);

  await storeCommits(commitKv, repo, testcommits);
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
