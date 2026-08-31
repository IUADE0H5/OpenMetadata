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

// Two mutually-exclusive entry paths, each dispatched via a dynamic
// `import()` so this entry file's static graph stays small. The silent-
// refresh iframe used to be a third branch here, but the URL now serves
// its own HTML (`silent-callback.html` — see `OpenMetadataAssetServlet`
// and `vite.config.ts`) so the SPA entry chunk never has to carry
// `oidc-client` or a `SilentCallback` React tree.
//
//   1. SSO "Test Login" popup — dynamic-import the test-login bootstrap
//      so it never touches the real AuthProvider or session storage.
//   2. Regular app boot — dynamic-import `BootstrapApp`, which pulls in
//      AppRoot, styles, i18n, and the core-components package.
if (isSsoTestLoginPopup()) {
  import('./components/SettingsSso/SsoTestLogin/ssoTestCallbackBootstrap')
    .then((module) => module.runSsoTestCallback())
    // If the chunk fails to load, close the popup so the opener doesn't hang.
    .catch(() => globalThis.close());
} else {
  recordPlaywrightAppBoot();
  void import('./BootstrapApp').then(({ bootstrapApp }) =>
    bootstrapApp(container)
  );
}

// Service-worker lifecycle -- registers the asset cache in prod, unregisters
// any stale one in dev where Vite HMR fights it.
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
