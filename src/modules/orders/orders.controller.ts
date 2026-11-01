import { Body, Controller, Get, Headers, Inject, Param, Post, Req, Res } from '@nestjs/common';
import { RouteSchema } from '@nestjs/platform-fastify';
import type { FastifyReply, FastifyRequest } from 'fastify';
import type { Pool } from 'pg';
import type { Static } from 'typebox';
import { PG_POOL } from '../../infrastructure/infrastructure.module.js';
import { acceptOrder } from './accept-order.js';
import { canonicalizeOrder } from './canonicalize-order.js';
import { getOrder } from './get-order.js';
import {
  AcceptedOrderResponse,
  CreateOrderBody,
  CreateOrderHeaders,
  OrderParams,
  OrderResponse,
} from './order.schemas.js';

export const LOCK_TIMEOUT_MS = Symbol('LockTimeoutMs');

/**
 * Authentication is out of scope, so every request shares one fixed scope. It is
 * never read from the request, which means no client can claim another identity.
 */
const DEMO_SCOPE = 'demo';

/** `RouteSchema` hands validation and serialization to Fastify, with the same schemas as ever. */
@Controller('v1/orders')
export class OrdersController {
  constructor(
    @Inject(PG_POOL) private readonly pool: Pool,
    @Inject(LOCK_TIMEOUT_MS) private readonly lockTimeoutMs: number,
  ) {}

  @Post()
  @RouteSchema({
    headers: CreateOrderHeaders,
    body: CreateOrderBody,
    response: { 202: AcceptedOrderResponse },
  })
  async create(
    @Headers('idempotency-key') idempotencyKey: string,
    @Body() order: Static<typeof CreateOrderBody>,
    @Req() request: FastifyRequest,
    // Taking the reply is how the receipt's stored status and location are sent as they are.
    @Res() reply: FastifyReply,
  ): Promise<void> {
    const result = await acceptOrder(this.pool, this.lockTimeoutMs, {
      scope: DEMO_SCOPE,
      idempotencyKey,
      order: canonicalizeOrder(order),
      correlationId: request.id,
    });

    const { status, location, body } = result.receipt;

    if (result.replayed) {
      request.log.info({ orderId: body.orderId }, 'order acceptance replayed');
    } else {
      const { orderId, eventId, symbol, acceptanceSequence } = result.event;
      request.log.info({ orderId, eventId, symbol, acceptanceSequence }, 'order accepted');
    }

    void reply.code(status).header('location', location).send(body);
  }

  @Get(':orderId')
  @RouteSchema({ params: OrderParams, response: { 200: OrderResponse } })
  find(@Param('orderId') orderId: string) {
    return getOrder(this.pool, DEMO_SCOPE, orderId);
  }
}
