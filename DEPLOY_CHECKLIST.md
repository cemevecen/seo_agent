# Deploy Verification Checklist

Use this list before and after triggering a production deploy.

## Before deploy

- Confirm your local branch is `main` and up to date with remote.
- Ensure the commit you want is on GitHub (`git log --oneline -1`).
- In the build panel, check current `Revision` (it must be older than your target commit if you are redeploying).

## During deploy

- Trigger a new build from `main`.
- Watch build logs for successful image export and publish steps.
- Verify no errors are reported in dependency install or app startup stages.

## After deploy

- Open build details and confirm `Revision` matches the intended commit SHA.
- Hard refresh the app in browser (`Cmd+Shift+R` on macOS).
- Trigger Data Explorer refresh and confirm chart data/shape changed.

