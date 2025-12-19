// git api
import { Octokit } from "npm:octokit";
import { Endpoints } from "@octokit/types";

type ListCommitsResponse = Endpoints["GET /repos/{owner}/{repo}/commits"]["response"]["data"];

// commit_timestamp, commit_hash, author_email, commit_message

type CommitData = {
  commitHash: string;
  commitTimestamp: string; // TODO: change later to some kind of date type
  commitMessage: string;
  authorEmail: string;
};

function parseCommits(commits: ListCommitsResponse): CommitData[] {
  const parsedCommits: CommitData[] = [];

  for (const item of commits) {
    const commit: CommitData = {
      commitHash: item.sha,
      commitTimestamp: item.commit.author.date,
      commitMessage: item.commit.message,
      authorEmail: item.commit.author.email,
    };

    parsedCommits.push(commit);
  }

  return parsedCommits;
}

async function storeCommits(kv: Deno.Kv, commits: CommitData[]): Promise<void> {
  for (const commit of commits) {
    const key = commit.commitHash;
    await kv.set(["commits", key], commit);
  }
}

async function getLatestCommitTimestamp(kv: Deno.Kv): Promise<string | undefined> {
  const commitIter = kv.list<CommitData>({ prefix: ["commits"] });
  let maxTimestamp: string | undefined = undefined;

  for await (const commit of commitIter) {
    const timestamp: string = commit.value.commitTimestamp;
    if (!maxTimestamp || timestamp > maxTimestamp) {
      maxTimestamp = timestamp;
    }
  }

  return maxTimestamp;
}

async function getCommits(octokit: Octokit, timestamp?: string): Promise<ListCommitsResponse> {
  // use paginate to handle larger repo commit history datasets
  const data  = await octokit.paginate("GET /repos/{owner}/{repo}/commits", {
    owner: "nakennedy11",
    repo: "cs4550hw01",
    since: timestamp
  });

  // TODO: make a clean log for undefined timestamp
  console.log(`${data.length} new commits found after timestamp ${timestamp}`);

  return data;
}

async function main(commitKv: Deno.Kv, octokit: Octokit): Promise<void> {
  const maxTs = await getLatestCommitTimestamp(commitKv);
  console.log(maxTs);

  const data = await getCommits(octokit, maxTs);

  const testcommits: CommitData[] = parseCommits(data);
  console.log(testcommits);

  await storeCommits(commitKv, testcommits);

  const maxTs2 = await getLatestCommitTimestamp(commitKv);
  console.log(maxTs2);
}

if (import.meta.main) {
  const commitKv = await Deno.openKv(".commitHistory.sqlite"); // TODO: make configurable
  const octokit = new Octokit();

  Deno.cron("Run loop to get and store commit history", "* * * * *", async () => {
    await main(commitKv, octokit);
  });
}
