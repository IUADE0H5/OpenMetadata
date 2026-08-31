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
import type { Root } from 'react-dom/client';

/**
 * Full-app bootstrap. Kept off the entry chunk (imported dynamically from
 * `src/index.tsx`) so the silent-callback iframe branch never pulls
 * `AppRoot`, the core-components package, or the app style sheet into its
 * bundle -- scenario 7 of SsoScenarios.spec asserts the resulting entry
 * chunk stays under 500 KB.
 *
 * `initCoreI18n` is intentionally invoked here (not inside `LocalUtil`) so
 * the core-components import doesn't leak into files that Playwright's
 * `--list` walks.
 */
export const bootstrapApp = async (root: Root): Promise<void> => {
  const [{ initCoreI18n }, { default: i18next }, { default: AppRoot }] =
    await Promise.all([
      import('@openmetadata/ui-core-components'),
      import('./utils/i18next/LocalUtil'),
      import('./AppRoot'),
      import('./styles/index'),
    ]);

  initCoreI18n(i18next);

  root.render(
    <React.StrictMode>
      <AppRoot />
    </React.StrictMode>
  );
};
