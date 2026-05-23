import { Controller, Post, Body, UseInterceptors, UploadedFile, BadRequestException, Get, Param } from '@nestjs/common';
import { AnalysisService } from './analysis.service';
import { FileInterceptor } from '@nestjs/platform-express';
import { AnalyzeImageDto } from './dto/analyze-image.dto';
import { memoryStorage } from 'multer';

@Controller('analysis')
export class AnalysisController {
  constructor(private readonly analysisService: AnalysisService) {}

  @Post()
  @UseInterceptors(FileInterceptor('image', { storage: memoryStorage() }))
  async analyze(@UploadedFile() file: Express.Multer.File, @Body() analyzeImageDto: AnalyzeImageDto) {
    if(!file) {
      throw new BadRequestException('Image file is required')
    }
    return this.analysisService.createJob(file, analyzeImageDto.userId);
  }

  @Get(':jobId')
  async getJob(@Param('jobId') jobId: string) {
    return this.analysisService.getJob(jobId);
  }
}
