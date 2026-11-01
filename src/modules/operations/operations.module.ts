import { Module, type DynamicModule } from '@nestjs/common';
import type { KafkaLag } from './kafka-lag.js';
import { OperationsController, READ_KAFKA_LAG } from './operations.controller.js';

@Module({})
export class OperationsModule {
  static register({ readKafkaLag }: { readKafkaLag: () => Promise<KafkaLag> }): DynamicModule {
    return {
      module: OperationsModule,
      controllers: [OperationsController],
      providers: [{ provide: READ_KAFKA_LAG, useValue: readKafkaLag }],
    };
  }
}
