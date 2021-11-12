import { Kafka} from   'kafkajs'

//il1
//const fromTopic= "il1-772124794258";
//  const fromTopic= "il1-prod-UserManagementNotificationsPartitioned";
// const toTopic ="il1-772124794258-s";
//
//
// const kafka = new Kafka({
//     clientId: 'my-app',
//     brokers: ['il1a-kfk4-br1:9092', 'il1a-kfk4-br2:9092']
// })

//eu1eu1a-kfk1-br1:9092,eu1b-kfk1-br2:9092
const fromTopic= "eu1-prod-UserManagementNotificationsPartitioned";
const toTopic ="cl-UEFA-8352404";


const kafka = new Kafka({
    clientId: 'cl-UEFA-8352404',
    brokers: ['eu1a-kfk1-br1:9092', 'eu1b-kfk1-br2:9092']
})
const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'cl-UEFA-eu1-dev' })
const site =  '8352404_';

const run = async () => {
    // Producing


    // Consuming
    await consumer.connect()
    await consumer.subscribe({ topic: fromTopic, fromBeginning: true })
    await producer.connect()

    await consumer.run({

        eachBatch: async ({
                              batch,
                              resolveOffset,
                              heartbeat,
                              commitOffsetsIfNecessary,
                              uncommittedOffsets,
                              isRunning,
                              isStale,
                          }) => {
            for (let message of batch.messages.filter((value, index) => value.key.toString().startsWith(site))) {
                let msgKey= message.key.toString();
                let msgValue= message.value.toString();
                console.log({
                    topic: batch.topic,
                    partition: batch.partition,
                    highWatermark: batch.highWatermark,
                    message: {
                        offset: message.offset,
                        key: msgKey,
                        value: msgValue,
                        headers: message.headers,
                    }
                })
                
                    // await producer.send({
                    //     topic: toTopic,
                    //     messages: [
                    //         {
                    //             headers: headers,
                    //             key: msgKey,
                    //             value:msgValue
                    //         },
                    //     ],
                    // })



                }
                resolveOffset(message.offset)
                await heartbeat()
            }
        }) ;
    
     };
 

run().catch(console.error)