import { Injectable } from '@nestjs/common';
import { PrismaService } from 'src/prisma/prisma.service';
import axios from 'axios';
import FormData = require('form-data');
import { RabbitmqService } from 'src/rabbitmq/rabbitmq.service';
import * as fs from 'fs';
import * as path from 'path';

const UPLOADS_DIR = '/uploads';


@Injectable()
export class AnalysisService {
  constructor(private prisma: PrismaService, private rabbitmq: RabbitmqService){}

  async createJob(file: Express.Multer.File, userId: string){
    const job = await this.prisma.job.create({
      data: {
        userId,
        mimeType: file.mimetype,
        sizeBytes: file.size
      }
    })

    const ext = path.extname(file.originalname) || '.jpg'
    const imagePath = path.join(UPLOADS_DIR, `${job.id}${ext}`)
    fs.writeFileSync(imagePath, file.buffer)

    await this.rabbitmq.publishJob(job.id, job.userId, imagePath)

    return {
      jobId: job.id,
      status: 'received',
    }
  }

  private async callWorker(file: Express.Multer.File, userId: string) {
    const form = new FormData();
    form.append('file', file.buffer, {
      filename: file.originalname,
      contentType: file.mimetype,
    });
    form.append('userId', userId);

    const response = await axios.post(
      `${process.env.WORKER_URL}/fingerprint`,
      form,
      { headers: form.getHeaders() },
    );

    return response.data;
  }
}
