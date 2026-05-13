import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib'

const QUEUE_NAME = 'analysis_jobs';


@Injectable()
export class RabbitmqService implements OnModuleInit, OnModuleDestroy {
    private connection: amqp.ChannelModel;
    private channel: amqp.Channel;

    async onModuleInit() {
        this.connection = await amqp.connect(process.env.RABBITMQ_URL!);
        this.channel = await this.connection.createChannel();
        await this.channel.assertQueue(QUEUE_NAME, { durable: true });
    }

    async onModuleDestroy() {
        await this.channel.close();
        await this.connection.close();
    }

    async publishJob(jobId: string, userId: string, imagePath: string) {
        const message = JSON.stringify({ jobId, userId, imagePath });
        this.channel.sendToQueue(QUEUE_NAME, Buffer.from(message), { persistent: true });
    }
}
