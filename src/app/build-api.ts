import { randomUUID } from 'node:crypto';
import { NestFactory } from '@nestjs/core';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import { LogController, type LogLevel } from 'fastify';
import { ApiModule, type ApiOptions } from './api.module.js';
import { NestLogger } from './nest-logger.js';

export async function buildApi(options: ApiOptions): Promise<NestFastifyApplication> {
  const adapter = new FastifyAdapter({
    loggerInstance: options.log,
    genReqId: () => randomUUID(),
    logController: new LogController({ requestIdLogLabel: 'requestId' }),
    bodyLimit: 16 * 1024,
    ajv: {
      customOptions: {
        // Fastify's defaults would turn the JSON number 100.5 into the string "100.5"
        // after it has passed through a float, and silently drop unknown properties.
        coerceTypes: false,
        removeAdditional: false,
      },
    },
    // No Nest middleware is used, so none of its plumbing should see a request.
    skipMiddie: true,
    // Fastify answers a malformed URL or an over-long parameter itself. Nest's default would
    // route them through the exception filter, which changes the headers and logs an error.
    // The types don't allow undefined, but the adapter's default has to be overwritten.
    frameworkErrors: undefined as never,
  });

  // Nest forwards a route's `config` but has no decorator for Fastify's route `logLevel`.
  adapter.getInstance().addHook('onRoute', (route) => {
    const { logLevel } = (route.config ?? {}) as { logLevel?: LogLevel };
    if (logLevel) {
      route.logLevel = logLevel;
    }
  });

  const app = await NestFactory.create<NestFastifyApplication>(
    ApiModule.forRoot(options),
    adapter,
    {
      // Nest would add a parser for form bodies, which this API answers with 415.
      bodyParser: false,
      // Let a failed start reject, rather than Nest ending the process itself.
      abortOnError: false,
      logger: new NestLogger(options.log),
    },
  );
  await app.init();
  await adapter.getInstance().ready();
  return app;
}
