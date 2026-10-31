import type { FastifyPluginCallbackTypebox } from '@fastify/type-provider-typebox';
import type { Pool } from 'pg';
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

/**
 * Authentication is out of scope, so every request shares one fixed scope. It is
 * never read from the request, which means no client can claim another identity.
 */
const DEMO_SCOPE = 'demo';

export interface OrderRoutesOptions {
  pool: Pool;
  lockTimeoutMs: number;
}

export const orderRoutes: FastifyPluginCallbackTypebox<OrderRoutesOptions> = (
  app,
  { pool, lockTimeoutMs },
  done,
) => {
  app.post(
    '/v1/orders',
    {
      schema: {
        headers: CreateOrderHeaders,
        body: CreateOrderBody,
        response: { 202: AcceptedOrderResponse },
      },
    },
    async (request, reply) => {
      const result = await acceptOrder(pool, lockTimeoutMs, {
        scope: DEMO_SCOPE,
        idempotencyKey: request.headers['idempotency-key'],
        order: canonicalizeOrder(request.body),
        correlationId: request.id,
      });

      const { status, location, body } = result.receipt;

      if (result.replayed) {
        request.log.info({ orderId: body.orderId }, 'order acceptance replayed');
      } else {
        const { orderId, eventId, symbol, acceptanceSequence } = result.event;
        request.log.info({ orderId, eventId, symbol, acceptanceSequence }, 'order accepted');
      }

      return reply.code(status).header('location', location).send(body);
    },
  );

  app.get(
    '/v1/orders/:orderId',
    {
      schema: {
        params: OrderParams,
        response: { 200: OrderResponse },
      },
    },
    (request) => getOrder(pool, DEMO_SCOPE, request.params.orderId),
  );

  done();
};
