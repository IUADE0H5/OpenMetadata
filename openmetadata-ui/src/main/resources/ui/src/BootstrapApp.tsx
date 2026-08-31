/*
 *  Copyright 2026 Collate.
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import React from 'react';
import { createRoot } from 'react-dom/client';

/**
 * Full-app bootstrap. Kept off the entry chunk (imported dynamically from
 * `src/index.tsx`) so the two dispatcher branches — this one and the
 * SSO test-login popup — each keep their heavy deps behind exactly one
 * dynamic import. `createRoot` lives here (not in `index.tsx`) so
 * `react-dom/client` never lands on the entry's static graph.
 *
 * The `/silent-callback` iframe route is now served by its own HTML
 * entry (`silent-callback.html` → `silentCallbackEntry.ts`) and never
 * runs this bootstrap.
 *
 * `initCoreI18n` is intentionally invoked here (not inside `LocalUtil`) so
 * the core-components import doesn't leak into files that Playwright's
 * `--list` walks.
 */
export const bootstrapApp = async (container: Element): Promise<void> => {
  const [{ initCoreI18n }, { default: i18next }, { default: AppRoot }] =
    await Promise.all([
      import('@openmetadata/ui-core-components'),
      import('./utils/i18next/LocalUtil'),
      import('./AppRoot'),
      import('./styles/index'),
    ]);

  initCoreI18n(i18next);

  createRoot(container).render(
    <React.StrictMode>
      <AppRoot />
    </React.StrictMode>
  );
};
