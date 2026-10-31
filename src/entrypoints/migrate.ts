import { loadConfig } from '../app/config.js';
import { applyMigrations } from '../infrastructure/migrate.js';

const applied = await applyMigrations(loadConfig().databaseUrl);

console.log(applied.length > 0 ? `Applied: ${applied.join(', ')}` : 'No pending migrations');
