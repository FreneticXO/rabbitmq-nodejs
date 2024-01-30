const amqp = require("amqplib");
const Redis =  require("redis");

const redisClient = Redis.createClient({
    host: '127.0.0.1',
    port: 6379
});



connect();


function process (x) {

    if(x == 0) return 0;
    if(x == 1) return 1;
    return process(x-1) + process(x-2);

}


async function connect () {
    try {

        await redisClient.connect();
        
        const connection = await amqp.connect("amqp://localhost:5672");
        const channel = await connection.createChannel();
        const result = await channel.assertQueue("jobs");

        channel.consume("jobs", async message => {
            const input = JSON.parse(message.content.toString());
            console.log(`Recieved JOB with input ${input.number}`);
            
            if(input.number <= 30) {
                const processed = await process(input.number);

                await redisClient.set(input.number, processed);
    
            }
            console.log(`Processing done for JOB ${input.number}`);

            if (input.number <99) {
                channel.ack(message);
            }
        })
        
        console.log("Waiting for messages");



    } catch (error) {
        console.error(error);
    }
}