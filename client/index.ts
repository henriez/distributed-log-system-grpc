import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import * as path from "path";
import inquirer from "inquirer";
import chalk from "chalk";
import figlet from "figlet";

const PROTO_PATH = path.join(__dirname, "..", "proto", "raft_client.proto");

const NODE_PORTS: Record<string, number> = {
  node1: 50051,
  node2: 50052,
  node3: 50053,
  node4: 50054,
};

const ALL_PORTS = Object.values(NODE_PORTS);

interface PublishResponse {
  success: boolean;
  leader_id: string;
  message: string;
}

interface ConsumeResponse {
  success: boolean;
  leader_id: string;
  commited_data: string[];
}

function randomPort(): number {
  return ALL_PORTS[Math.floor(Math.random() * ALL_PORTS.length)];
}

function createClient(port: number) {
  const target = `localhost:${port}`;
  const packageDef = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    defaults: true,
    oneofs: true,
  });
  const proto = grpc.loadPackageDefinition(packageDef) as any;
  const client = new proto.raft.client.RaftClientAPI(target, grpc.credentials.createInsecure());
  return { client, target };
}

function publish(client: any, data: string): Promise<PublishResponse> {
  return new Promise((resolve, reject) => {
    client.PublishData({ data }, (err: any, resp: PublishResponse) => {
      if (err) reject(err);
      else resolve(resp);
    });
  });
}

function consume(client: any): Promise<ConsumeResponse> {
  return new Promise((resolve, reject) => {
    client.ConsumeData({}, (err: any, resp: ConsumeResponse) => {
      if (err) reject(err);
      else resolve(resp);
    });
  });
}

function leaderPort(leaderId: string): number | null {
  return NODE_PORTS[leaderId] ?? null;
}

async function executeWithFailover(
  operation: "publish" | "consume",
  data: string | null,
  initialPort: number,
  initialClient: any
): Promise<{ resp?: any; client: any; port: number; error?: string }> {
  let port = initialPort;
  let client = initialClient;

  for (let i = 0; i < ALL_PORTS.length; i++) {
    try {
      const resp = operation === "publish" ? await publish(client, data!) : await consume(client);
      return { resp, client, port };
    } catch (err: any) {
      console.log(chalk.red(`[!] Failed communicating with port ${port}. Trying other nodes...`));
      port = ALL_PORTS[(ALL_PORTS.indexOf(port) + 1) % ALL_PORTS.length];
      client = createClient(port).client;
    }
  }
  return { client, port, error: "All nodes are unnacessible." };
}

async function main() {
  let port = randomPort();
  let { client, target } = createClient(port);

  const clearScreen = () => {
    process.stdout.write('\x1Bc');
    console.log(chalk.cyan(figlet.textSync('RAFT', { font: 'Standard' })));
    console.log(chalk.gray(`Connected to: ${target} | Status: Online\n`));
  };
  clearScreen();

  while (true) {
    const { action } = await inquirer.prompt([{
      type: 'rawlist',
      name: 'action',
      message: 'Select an operation:',
      choices: [
        { name: 'Publish log', value: 'p' },
        { name: 'Query commited logs', value: 'c' },
        { name: chalk.red('Quit'), value: 'q' }
      ]
    }]);

    if (action === 'q') break;

    if (action === 'p') {
      const { data } = await inquirer.prompt([{ type: 'input', name: 'data', message: 'Input content:' }]);
      if (!data) continue;

      const result = await executeWithFailover("publish", data, port, client);
      if (result.error) {
        console.log(chalk.red(`[!] ${result.error}`));
        continue;
      }
      
      
      port = result.port;
      client = result.client;
      let resp = result.resp as PublishResponse;

      if (resp.success) {
        console.log(chalk.green(`Published: "${data}"`));
      } else if (resp.leader_id) {
        const newPort = leaderPort(resp.leader_id);
        if (newPort && newPort !== port) {
          port = newPort;
          client = createClient(port).client;
          console.log(chalk.blue(`Redirected to ${resp.leader_id} on port ${port}`));
          try {
            resp = await publish(client, data);
            if (resp.success) console.log(chalk.green(`Published: "${data}"`));
            else console.log(chalk.red(`Publish failed: ${resp.message}`));
          } catch (err: any) {
             console.log(chalk.red(`RPC error on redirect: ${err.message}`));
          }
        } else {
          console.log(chalk.red(`Publish failed, no redirect available`));
        }
      } else {
        console.log(chalk.red(`Publish failed: ${resp.message}`));
      }
    }

    if (action === 'c') {
      const result = await executeWithFailover("consume", null, port, client);

      if (result.error) {
        console.log(chalk.red(`[!] ${result.error}`));
        await inquirer.prompt([{ type: 'input', name: 'pause', message: 'Press Enter to continue...' }]);
        clearScreen();
        continue;
      }

      let resp = result.resp as ConsumeResponse;
      if (resp.success) {
        if (resp.commited_data.length === 0) console.log(chalk.blue("No committed data yet"));
        else {
          console.log(chalk.blue("Committed data:"));
          resp.commited_data.forEach((d, i) => console.log(chalk.blue(`  ${i + 1}. ${d}`)));
        }
      } else if (resp.leader_id) {
        const newPort = leaderPort(resp.leader_id);
        if (newPort && newPort !== port) {
          port = newPort;
          client = createClient(port).client;
          console.log(chalk.green(`Redirected to ${resp.leader_id} on port ${port}`));
          try {
            resp = await consume(client);
            if (resp.success) {
              if (resp.commited_data.length === 0) console.log(chalk.blue("No committed data yet"));
              else {
                console.log(chalk.blue("Committed data:"));
                resp.commited_data.forEach((d, i) => console.log(chalk.blue(`  ${i + 1}. ${d}`)));
              }
            } else console.log(chalk.red("Read failed"));
          } catch (err: any) {
             console.log(chalk.red(`RPC error on redirect: ${err.message}`));
          }
        } else {
          console.log(chalk.red("Read failed, no redirect available"));
        }
      } else {
        console.log(chalk.red("Read failed, leader unknown"));
      }
    }

    await inquirer.prompt([{ type: 'input', name: 'pause', message: 'Press Enter to continue...' }]);
      clearScreen();
  }
  console.log(chalk.yellow("Shutting down client..."));
  process.exit(0);
}

main();