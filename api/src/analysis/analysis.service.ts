import { Injectable } from '@nestjs/common';
import { PrismaService } from 'src/prisma/prisma.service';
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

  async getJob(jobId: string) {
    const job = await this.prisma.job.findUnique({
      where: { id: jobId }
    });
    return job;
  }
}
