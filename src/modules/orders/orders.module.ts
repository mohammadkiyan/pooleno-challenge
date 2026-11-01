import { Module, type DynamicModule } from '@nestjs/common';
import { LOCK_TIMEOUT_MS, OrdersController } from './orders.controller.js';

@Module({})
export class OrdersModule {
  static register({ lockTimeoutMs }: { lockTimeoutMs: number }): DynamicModule {
    return {
      module: OrdersModule,
      controllers: [OrdersController],
      providers: [{ provide: LOCK_TIMEOUT_MS, useValue: lockTimeoutMs }],
    };
  }
}
