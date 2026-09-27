# Sanjay Ki Duniya — phone/cloud setup

## What this version does
- Runs Flask on the public Render port (`0.0.0.0:$PORT`).
- Keeps the Upstox token in a server environment variable, not in the phone UI.
- Includes a PWA manifest, service worker and install icons so the site can be added to an Android home screen.
- API calls stay network-first; cached files are only for the app shell.

## Render deployment
1. Put this folder in a GitHub repository.
2. In Render choose **New → Web Service**, connect the repository, and choose the Free plan.
3. Build command: `pip install -r requirements.txt gunicorn`
4. Start command: `gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120 server:app`
5. Add secret environment variable `UPSTOX_ACCESS_TOKEN` in Render. Never put the token in `index.html` or commit it to GitHub.
6. Deploy. Render gives an `onrender.com` HTTPS URL.
7. Open that URL on Android Chrome → browser menu → **Add to Home screen / Install app**.

## Important free-tier behavior
Render free web services can sleep after 15 minutes without inbound traffic and local SQLite files are ephemeral. This means research history stored in the local SQLite database should not be treated as permanent cloud storage on the free tier.
