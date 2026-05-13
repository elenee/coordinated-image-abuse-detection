import { Module } from '@nestjs/common';
import { AnalysisService } from './analysis.service';
import { AnalysisController } from './analysis.controller';
import { RabbitmqService } from 'src/rabbitmq/rabbitmq.service';

@Module({
  controllers: [AnalysisController],
  providers: [AnalysisService, RabbitmqService],
})
export class AnalysisModule {}
