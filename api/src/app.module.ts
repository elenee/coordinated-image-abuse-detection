import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { AnalysisModule } from './analysis/analysis.module';
import { PrismaModule } from './prisma/prisma.module';
import {ConfigModule} from '@nestjs/config'
import { RabbitmqModule } from './rabbitmq/rabbitmq.module';

@Module({
  imports: [AnalysisModule, PrismaModule,
    ConfigModule.forRoot({ isGlobal: true }),
    RabbitmqModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
