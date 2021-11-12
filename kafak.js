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
    brokers: ['eu1eu1a-kfk1-br1:9092', 'eu1b-kfk1-br2:9092']
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
            if(msgKey.startsWith(site) ) {
                console.log({
                    site:site,
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