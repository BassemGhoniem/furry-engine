const Twitter = require('twitter');
const {
    Kafka,
    logLevel,
    CompressionTypes,
    CompressionCodecs
} = require('kafkajs')

const SnappyCodec = require('kafkajs-snappy')
CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec

class TwitterProducer {
    constructor(tag) {

        if (!tag)
            throw new Error('You must provide tag to track');

        this.tag = tag;
        this.kafkaTopic = `${this.tag}_tweets_stream`;
        this.twitterClient = new Twitter({
            consumer_key: process.env.TWITTER_CONSUMER_KEY,
            consumer_secret: process.env.TWITTER_CONSUMER_SECRET,
            access_token_key: process.env.TWITTER_ACCESS_TOKEN_KEY,
            access_token_secret: process.env.TWITTER_ACCESS_TOKEN_SECRET
        });

        this.kafkaClient = new Kafka({
            clientId: 'my-app',
            brokers: [process.env.KAFKA_BROKER],
            logLevel: logLevel.NOTHING
        });
    }

    async run() {
        const kafkaAdmin = this.kafkaClient.admin();
        await kafkaAdmin.connect();
        await kafkaAdmin.createTopics({
            topics: [{
                topic: this.kafkaTopic,
                numPartitions: 3
            }]
        });
        await kafkaAdmin.disconnect();

        const producer = this.kafkaClient.producer();
        await producer.connect()

        const tweetsStream = this.twitterClient.stream('statuses/filter', { track: this.tag });

        tweetsStream.on('error', error => {
            throw error;
        });


        tweetsStream.on('data', data => {
            console.log('\n\n\n', data.text);
            producer.send({
                topic: this.kafkaTopic,
                messages: [{ value: JSON.stringify(data, null, 2) }],
                compression: CompressionTypes.Snappy,
            })
        });
    }
}

module.exports = TwitterProducer;