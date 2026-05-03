import { Injectable } from '@nestjs/common';
import { PrismaService } from 'src/prisma/prisma.service';

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

    return {
      jobId: job.id,
      status: 'received',
    }
  }
}
