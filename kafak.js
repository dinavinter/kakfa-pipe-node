import { Kafka} from   'kafkajs'
const fromTopic= "il1-prod-UserManagementNotificationsPartitioned";
const toTopic ="il1-prod-ChangelogValidEventBus";
const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['il1a-kfk4-br1:9092', 'il1a-kfk4-br2:9092']
})

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'test-group-2' })

const run = async () => {
    // Producing


    // Consuming
    await consumer.connect()
    await consumer.subscribe({ topic: fromTopic, fromBeginning: true })
    await producer.connect()

    await consumer.run({ 
        eachMessage: async ({ topic, partition, message }) => {
            var msgValue=message.value.toString();
            var msgKey=message.key.toString();
            var headers=message.headers.toString();
            console.log({
                partition,
                msgKey,
                headers,
                offset: message.offset,
                value: msgValue,
            })
            if(msgValue.indexOf('847053487682') >0 ) {
                console.log({
                    site:847053487682,
                    partition,
                    msgKey,
                    headers,
                    offset: message.offset,
                    value: msgValue,
                })
                await producer.send({
                    topic: toTopic,
                    messages: [
                        {
                            headers: headers,
                            key: msgKey,
                            value:msgValue
                         },
                    ],
                })
             
            }
        },
    })
}

run().catch(console.error)