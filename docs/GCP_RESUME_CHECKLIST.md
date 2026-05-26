# 🚀 When You Return — GCP Deploy Checklist

After getting a Gmail without org policies (family member's, or a brand-new one):

---

## Step 1 — In a browser (5 min)

1. Open **Incognito window**
2. Go to https://console.cloud.google.com
3. Sign in with the new Gmail
4. Click "Activate" on the free trial → enter card → get $300 credit
5. Top-left dropdown → **New Project**
6. **⚠️ CRITICAL** — In the form, find the **"Location"** field
   - It MUST show: `📁 No organization`
   - If it shows anything else, this Gmail is also org-bound — try yet another
7. Project name: `fraud-detection-sandbox` (or anything you like)
8. Wait ~30 sec for project creation
9. **Copy the Project ID** shown on the dashboard (looks like `fraud-detection-sandbox-123456`)
10. Go to "Billing" in the left menu → ensure billing is linked
11. Run in terminal: `gcloud billing accounts list` to grab your new Billing Account ID

---

## Step 2 — Tell Sweety

Come back to the chat and paste:

```
new project id: <YOUR-PROJECT-ID>
new billing account id: <YOUR-BILLING-ACCOUNT-ID>
new gmail: <THE-GMAIL>
```

Sweety will:
1. Run `gcloud auth login` (you'll re-auth in browser with new Gmail)
2. Run `gcloud auth application-default login` (same browser dance)
3. Update `~/.zshrc` with the new env vars
4. Run `./gcp/setup.sh` to create GCS bucket + BigQuery + Service Account + $25 budget
5. Run `./gcp/trigger_pipeline.sh` for your FIRST CLOUD FRAUD DETECTION RUN 🎉

---

## Verification before saying "ready to Sweety"

In your terminal:

```bash
# Should print the new email
gcloud auth list --filter="status:ACTIVE" --format="value(account)"

# Should print NOTHING (empty = no org = good)
gcloud projects describe YOUR-NEW-PROJECT-ID --format='value(parent)'

# Should NOT 403
gsutil ls
```

If `gsutil ls` works without error → you're golden! 🐶
If it 403s with VPC SC error → this Gmail is also blocked, try a different one

---

## Meanwhile (while you wait for the right Gmail)

Phase 1 still runs end-to-end. You can:
- Run `./scripts/verify.sh` to re-check local results
- Read `docs/phase1_walkthrough.html` for reference
- Read `docs/phase2_gcp_deploy.html` for what Phase 2 will look like
