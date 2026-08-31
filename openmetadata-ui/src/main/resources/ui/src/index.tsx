/*
 *  Copyright 2022 Collate.
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
import SilentCallback from './components/Auth/SilentCallback';
import { isSilentCallbackRoute } from './components/Auth/SilentCallback/isSilentCallbackRoute';
import { getBasePath } from './utils/HistoryUtils';
import { isSsoTestLoginPopup } from './utils/SsoTestLoginPopup';

const recordPlaywrightAppBoot = () => {
  if (!import.meta.env.PW_E2E_BUILD) {
    return;
  }

  const scenarioKey = 'playwright-ui-scenario';
  const isNewScenario = !sessionStorage.getItem(scenarioKey);
  if (isNewScenario) {
    sessionStorage.setItem(scenarioKey, '1');
  }

  const basePath = getBasePath();
  const diagnostics = new URLSearchParams({ 'playwright-app-boot': '1' });
  if (isNewScenario) {
    diagnostics.set('playwright-ui-scenario', '1');
  }
  void fetch(`${basePath}/favicon.ico?${diagnostics}`, {
    cache: 'no-store',
    credentials: 'same-origin',
    keepalive: true,
  }).catch(() => {
    if (isNewScenario) {
      sessionStorage.removeItem(scenarioKey);
    }
  });
};

const container = document.getElementById('root');
if (!container) {
  throw new Error('Failed to find the root element');
}

const root = createRoot(container);
const silentCallbackRoute = isSilentCallbackRoute();

// Three mutually-exclusive entry paths, dispatched from a single spot so the
// bundle graph reflects the intent:
//
//   1. Silent-refresh iframe — render the tiny SilentCallback shim and
//      nothing else. AppRoot + its deps stay off this chunk (scenario 7 of
//      SsoScenarios.spec caps the JS payload on `/silent-callback`).
//   2. SSO "Test Login" popup — dynamic import the test-login bootstrap so
//      it never touches the real AuthProvider or session storage.
//   3. Regular app boot — dynamic import BootstrapApp, which pulls in
//      AppRoot, styles, i18n, and the core-components package.
if (silentCallbackRoute) {
  root.render(
    <React.StrictMode>
      <SilentCallback />
    </React.StrictMode>
  );
} else if (isSsoTestLoginPopup()) {
  import('./components/SettingsSso/SsoTestLogin/ssoTestCallbackBootstrap')
    .then((module) => module.runSsoTestCallback())
    // If the chunk fails to load, close the popup so the opener doesn't hang.
    .catch(() => globalThis.close());
} else {
  recordPlaywrightAppBoot();
  void import('./BootstrapApp').then(({ bootstrapApp }) => bootstrapApp(root));
}

// Service-worker lifecycle -- registers the asset cache in prod, unregisters
// any stale one in dev where Vite HMR fights it. Skipped on the
// silent-callback path: the iframe has no need for the cache, and
// unregistering there would tear down the parent tab's cached assets.
if (!silentCallbackRoute) {
  if (import.meta.env.DEV) {
    if ('serviceWorker' in navigator) {
      navigator.serviceWorker
        .getRegistrations()
        .then((registrations) =>
          registrations.forEach((registration) => registration.unregister())
        );
    }
  } else if ('serviceWorker' in navigator && 'indexedDB' in globalThis) {
    window.addEventListener('load', () => {
      const basePath = getBasePath();
      const serviceWorkerPath = basePath
        ? `${basePath}/app-worker.js`
        : '/app-worker.js';
      navigator.serviceWorker.register(serviceWorkerPath);
    });
  }
}
