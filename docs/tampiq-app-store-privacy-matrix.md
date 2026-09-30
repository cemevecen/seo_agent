# TAMPIQ — App Store Connect Privacy answers (INTERNAL)

**Not published on the public Help Center.**  
Aligned with production iOS audit (PrivacyInfo empty collected types, no tracking, local-first, no Tampiq backend telemetry). Effective reference date: 30 September 2026.

## Overall recommendation

| Question | Recommended answer |
|----------|-------------------|
| Do you or your third-party partners collect data from this app? | **No — Data Not Collected** |
| Tracking? | **No** |
| Privacy Nutrition Labels data types | **None declared** (matches `NSPrivacyCollectedDataTypes: []`) |

If legal later requires declaring on-device optional location/motion as “collected,” keep Connect, PrivacyInfo, and the public Privacy Policy consistent—do not diverge.

## Per Apple data category

| Category | Collected from user? | Linked to user? | Used for tracking? | Purpose |
|----------|---------------------|-----------------|--------------------|---------|
| Contact Info | No | — | No | — |
| Health & Fitness | No* | — | No | Motion used on-device for protection evidence; not HealthKit. Prefer “Not Collected” under Apple off-device definition |
| Financial Info | No | — | No | — |
| Location | No* (on-device optional) | — | No | App Functionality if ever declared; default **Not Collected** |
| Sensitive Info | No | — | No | — |
| Contacts | No | — | No | — |
| User Content | No | — | No | — |
| Browsing History | No | — | No | — |
| Search History | No | — | No | — |
| Identifiers | No | — | No | No IDFA/IDFV product analytics |
| Purchases | No | — | No | — |
| Usage Data | No* (on-device session journal) | — | No | Prefer **Not Collected** |
| Diagnostics | No | — | No | No crash/analytics SDK |
| Other Data | No | — | No | — |

\*Apple’s “collect” generally means data transmitted off device / accessible to the developer off device. Tampiq keeps protection journals on-device; PrivacyInfo declares no collected data types.

## Support correspondence (outside App Privacy labels)

Email sent to support/privacy addresses is processed for customer service. It is not app telemetry and is not declared as in-app data collection.

## Product URLs for App Store Connect

- Support: `https://tampiq-support-production.up.railway.app/support`
- Privacy: `https://tampiq-support-production.up.railway.app/privacy`
- Optional language handoff: append `?lang=` with one of: `tr`, `en-US`, `ar`, `zh-Hant`, `fr`, `de`, `hi`, `ja`, `pt-BR`, `es-ES`, `ur`

## Store metadata note (do not change iOS binary in this task)

Existing store asset support URL files still point at `https://ivicinlab.com/tampiq/support`.  
Recommended App Store / metadata update (when authorized): replace with the Railway Support URL above (optionally with `?lang=` matching listing locale).
