# Optional extras

This directory contains optional supporting material outside the main code challenge scope. Reviewers can evaluate the challenge without setting up anything here.

## Scenario viewer

An independent React, TypeScript, Vite, and Motion frontend with six one-click animated walkthroughs:

- Price priority, FIFO, and partial fills.
- Non-crossing limit orders.
- Identical requests under one idempotency key.
- A changed request under an existing key.
- A notification applied by the destination whose response is lost.
- Durable acceptance while Kafka is unavailable, followed by recovery.

**This is an illustrated frontend preview.** The scenarios use authored example frames, not captured backend traffic. Running them sends no order requests, changes no database, and controls no containers. The interface labels its examples and expected outcomes accordingly. Live scenario orchestration is a separate future step.

## Run

From the repository root:

```bash
cd extras
npm ci
npm run dev
```

Open [the scenario viewer](http://127.0.0.1:5173). Click a scenario to start its illustration, or use **Run scenario** for the initially selected one. Pause, resume, move between steps, change playback speed, and replay without backend setup. Reduced-motion preferences are respected.

```bash
npm test
npm run build
npm run preview
```

The preview command serves the production build on Vite's preview port, normally `4173`.

For the browser smoke tests, install Playwright's Chromium once, then run:

```bash
npx playwright install chromium
npm run test:browser
```

The browser checks exercise all six scenario buttons, matching playback, replay and pause, the delivery retry path, keyboard tabs, mobile widths, and reduced motion. They verify the frontend illustration, not backend guarantees.

## Structure

```text
extras/
  frontend/
    App.tsx          Scenario selection, system flow, book, and example details
    scenarios.ts    Authored explanations and illustrative frames
    playback.ts     Playback state and transitions
    styles.css      Responsive layout and reduced-motion styles
    main.tsx        Frontend entry point
  tests/
    playback.test.ts
  package.json      Independent dependencies and commands
  vite.config.ts
```

The main backend has no imports or runtime dependency on this directory. Its root lint configuration excludes `extras/`, so reviewers can run challenge checks without installing the optional frontend. All frontend code, interfaces, configuration, generated files, and its lockfile stay here.
