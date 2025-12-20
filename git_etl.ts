// git api
import { Octokit } from "npm:octokit";
import { Endpoints } from "@octokit/types";
import { SECOND } from "@std/datetime";
import { CronExpressionParser } from "npm:cron-parser";

import configFile from "./config.json" with { type: "json" };

type ListCommitsResponse = Endpoints["GET /repos/{owner}/{repo}/commits"]["response"]["data"];

type CommitData = {
  commitHash: string;
  commitTimestamp: Date | undefined;
  commitMessage: string;
  authorEmail: string | undefined;
};

type RunOptions = {
  repo: string | undefined;
  owner: string | undefined;
  cronSchedule: string | undefined;
  kvPath: string | undefined;
  clearKvOnStartup: string | undefined;
};

async function getCommits(octokit: Octokit, timestamp?: Date): Promise<ListCommitsResponse> {
  // use paginate to handle larger repo commit history datasets
  const data = await octokit.paginate("GET /repos/{owner}/{repo}/commits", {
    owner: "nakennedy11",
    repo: "cs4550hw01",
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

async function storeCommits(kv: Deno.Kv, commits: CommitData[]): Promise<void> {
  for (const commit of commits) {
    const key = commit.commitHash;
    await kv.set(["commits", key], commit);
  }
}

async function getLatestCommitTimestamp(kv: Deno.Kv): Promise<Date | undefined> {
  const commitIter = kv.list<CommitData>({ prefix: ["commits"] });
  let maxTimestamp: Date | undefined = undefined;

  for await (const commit of commitIter) {
    const timestamp: Date | undefined = commit.value.commitTimestamp;
    if (!maxTimestamp || (timestamp && timestamp > maxTimestamp)) {
      maxTimestamp = timestamp;
    }
  }

  return maxTimestamp;
}

async function main(commitKv: Deno.Kv, octokit: Octokit): Promise<void> {
  let maxTs = await getLatestCommitTimestamp(commitKv);

  if (maxTs) {
    // increment maximum commit timestamp by 1 second to get only new commits
    maxTs = new Date(maxTs.getTime() + SECOND);
  }

  const data = await getCommits(octokit, maxTs);

  const testcommits: CommitData[] = parseCommits(data);
  console.log(testcommits);

  await storeCommits(commitKv, testcommits);
}

function readConfig(): RunOptions {

  // set default values of run options
  let repo: string = "cs4550hw01";
  let owner: string = "nakennedy11";
  let cronSchedule: string = "* * * * *";
  let kvPath: string = "./kv/commitHistory.sqlite";
  let clearKvOnStartup: string = "Y";

  // validate inputs

  // 
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
        console.log("Error parsing cron expression:", err.message);
      }
    }
  }

  // filepath validation? should end in sqllite
  

  // clearKvOnStartup should be Y or N
  if (configFile.clearKvOnStartup && configFile.clearKvOnStartup in ["Y", "N"]) {
    clearKvOnStartup = configFile.clearKvOnStartup;
  }

  return {
    repo: repo,
    owner: owner,
    cronSchedule: cronSchedule,
    kvPath: kvPath,
    clearKvOnStartup: clearKvOnStartup,
  };
}

if (import.meta.main) {
  const commitKv = await Deno.openKv(".commitHistory.sqlite"); // TODO: make configurable
  const octokit = new Octokit();

  Deno.cron("Run loop to get and store commit history", "* * * * *", async () => {
    await main(commitKv, octokit);
  });
}
