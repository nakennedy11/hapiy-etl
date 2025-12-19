// git api
import { Octokit } from "https://esm.sh/@octokit/core@5";



const resp = await fetch("https://example.com");


console.log(resp.status); // 200
console.log(resp.headers.get("Content-Type")); // "text/html"
console.log(await resp.text()); // "Hello, World!"


const octokit = new Octokit();

const { data } = await octokit.request('GET /repos/{owner}/{repo}/commits', {
  owner: 'nakennedy11',
  repo: 'cs4550hw01',
  headers: {
    'X-GitHub-Api-Version': '2022-11-28'
  }
});

console.log(data);



