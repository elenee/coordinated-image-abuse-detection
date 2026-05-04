import { Injectable } from '@nestjs/common';
import { PrismaService } from 'src/prisma/prisma.service';
import axios from 'axios';
import FormData = require('form-data');

@Injectable()
export class AnalysisService {
  constructor(private prisma: PrismaService){}

  async createJob(file: Express.Multer.File, userId: string){
    const job = await this.prisma.job.create({
      data: {
        userId,
        mimeType: file.mimetype,
        sizeBytes: file.size
      }
    })

     const fingerprint = await this.callWorker(file, userId);

    return {
      jobId: job.id,
      status: 'received',
      fingerprint
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
