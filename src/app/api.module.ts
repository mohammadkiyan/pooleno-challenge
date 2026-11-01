import { Module, type DynamicModule } from '@nestjs/common';
import { APP_FILTER } from '@nestjs/core';
import type { Logger } from 'pino';
import { InfrastructureModule } from '../infrastructure/infrastructure.module.js';
import type { KafkaLag } from '../modules/operations/kafka-lag.js';
import { OperationsModule } from '../modules/operations/operations.module.js';
import { OrdersModule } from '../modules/orders/orders.module.js';
import { ApiExceptionFilter } from './api-exception.filter.js';

export interface ApiOptions {
  databaseUrl: string;
  log: Logger;
  lockTimeoutMs: number;
  readKafkaLag: () => Promise<KafkaLag>;
}

@Module({})
export class ApiModule {
  static forRoot({ databaseUrl, log, lockTimeoutMs, readKafkaLag }: ApiOptions): DynamicModule {
    return {
      module: ApiModule,
      imports: [
        InfrastructureModule.forRoot({ databaseUrl, log }),
        OrdersModule.register({ lockTimeoutMs }),
        OperationsModule.register({ readKafkaLag }),
      ],
      providers: [{ provide: APP_FILTER, useClass: ApiExceptionFilter }],
    };
  }
}
