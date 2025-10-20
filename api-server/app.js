import express from 'express'
import { generateSlug } from 'random-word-slugs'
import { ECSClient, RunTaskCommand } from '@aws-sdk/client-ecs'
import { Kafka } from 'kafkajs'
import { createClient as supabaseclient } from '@supabase/supabase-js'
import { v4 as uuidv4 } from 'uuid'
import fs from 'fs'
import { createClient } from '@clickhouse/client'
import path from 'path'
import { fileURLToPath } from 'url'
import 'dotenv/config';

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

const app = express()
const PORT = 9000
const supabaseUrl = process.env.SUPABASE_URL
const supabaseKey = process.env.SUPABASE_KEY;
console.log(typeof supabaseUrl,typeof supabaseKey)
const supabase = supabaseclient(supabaseUrl, supabaseKey)
const kafka = new Kafka({
    clientId: `api-server`,
    brokers: [process.env.KAFKA_BROKER],
    ssl: {
        ca: [fs.readFileSync(path.join(__dirname, 'kafka.pem'), 'utf-8')]
    },
    sasl: {
        username: 'avnadmin',
        password: process.env.KAFKA_PASSWORD,
        mechanism: 'plain'
    }

})

const client = createClient({
    host: process.env.CLICKHOUSE_HOST,
    database: 'default',
    username: 'avnadmin',
    password: process.env.CLICKHOUSE_PASSWORD
})

const consumer = kafka.consumer({ groupId: 'api-server-logs-consumer' })




const ecsClient = new ECSClient({
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY

    }
})

const config = {
    CLUSTER: 'arn:aws:ecs:ap-south-1:090172996198:cluster/builder-cluster',
    TASK: 'arn:aws:ecs:ap-south-1:090172996198:task-definition/builder-task:9'
}

app.use(express.json())

app.post("/project",async (req, res) => {
    const {name, gitURL,customDomain} = req.body
  
    const { data,error } = await supabase
  .from('project')
  .insert({ name: name, git_url: gitURL,sub_domain: generateSlug() ,custom_domain: customDomain?customDomain:null}).select()
    if (error) {
        res.status(500).json({ error: error.message })
    }
    res.status(200).json({ message: 'Project created successfully' ,data})

})

app.post('/deploy', async (req, res) => {
    const {projectId} = req.body
    const { data: projects, err } = await supabase.from('project').select().eq('id', projectId)
    const project = projects[0]
    console.log(" projectdetails",project)
    
  
    // Spin the container
    const { data: deployments ,error } = await supabase.from("project").update({ status: 'QUEUED' }).eq('id', projectId).select()
    const deployment = deployments[0]
    console.log(deployment)
    const command = new RunTaskCommand({
        cluster: config.CLUSTER,
        taskDefinition: config.TASK,
        launchType: 'FARGATE',
        count: 1,
        networkConfiguration: {
            awsvpcConfiguration: {
                assignPublicIp: 'ENABLED',
                subnets: ['subnet-0a4059dbe017b3412', 'subnet-05d1884e1dce31a1b', 'subnet-029ae423d3f6b6523'],
                securityGroups: ['sg-085f0f0d254f3a4ae']
            }
        },
        overrides: {
            containerOverrides: [
                {
                    name: 'builder-image',
                    environment: [
                        { name: 'GIT_REPO', value: project.git_url },
                        { name: 'PROJECT_ID', value: project.id}
                        ]
                }
            ]
        }
    })

    await ecsClient.send(command);

    return res.json({ status: 'queued', data: { deployment, url: `http://${deployment.sub_domain}.localhost:8000` } })

})


app.get('/logs/:id', async (req, res) => {
    const id = req.params.id;
    const logs = await client.query({
        query: `SELECT event_id, deployment_id, log, timestamp from log_events where deployment_id = {deployment_id:String}`,
        query_params: {
            deployment_id: id
        },
        format: 'JSONEachRow'
    })

    const rawLogs = await logs.json()

    return res.json({ logs: rawLogs })
})


async function initkafkaConsumer() {
    await consumer.connect();
    await consumer.subscribe({ topics: ['container-logs'], fromBeginning: true })

    await consumer.run({

        eachBatch: async function ({ batch, heartbeat, commitOffsetsIfNecessary, resolveOffset }) {

            const messages = batch.messages;
            console.log(`Recv. ${messages.length} messages..`)
            for (const message of messages) {
                if (!message.value) continue;
                const stringMessage = message.value.toString()
                const { PROJECT_ID, log } = JSON.parse(stringMessage)
                console.log({ log, PROJECT_ID })
                try {
                    const { query_id } = await client.insert({
                        table: 'log_events',
                        values: [{ event_id: uuidv4(), deployment_id: PROJECT_ID, log }],
                        format: 'JSONEachRow'
                    })
                    console.log(query_id)
                    resolveOffset(message.offset)
                    await commitOffsetsIfNecessary(message.offset)
                    await heartbeat()
                } catch (err) {
                    console.log(err)
                }

            }
        }
    })
}


initkafkaConsumer()


app.listen(PORT, () => console.log(`API Server Running..${PORT}`))