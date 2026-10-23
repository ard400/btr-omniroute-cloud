# HIDRO64 WhatsApp bridge

Node service for one QR-linked WhatsApp device. Baileys is pinned to the patched stable 6.7.24. No outbound-message API, history import, contact import or media download. Chats require an explicit allowlist.

Runtime: Node 22+. Install with `npm ci --omit=dev`; start with `npm start`. Configure `HIDRO_WA_ENDPOINT` as the HTTPS `/api/whatsapp-bridge` URL of the HIDRO64 Worker and `HIDRO_WA_BRIDGE_TOKEN` as a runtime secret of at least 32 characters. Never commit credentials.

Authentication credentials and Signal keys are stored by the Worker with durable CAS revisions; they must be encrypted there. QR values are emitted only to the authenticated Worker and expire within 55 seconds. Public `GET /health` exposes no session data. All `/session` controls require the Bearer token. `POST /session/start` allows linking with an empty allowlist without importing any chat. `POST /session/stop` disconnects the process while keeping the linked device; `GET /session` returns state without QR/auth.

The code does not prove a live connection: a real QR requires a deployed process and WhatsApp response; connected status requires the user to scan it. Render Free can sleep and shares monthly quotas with other services. No 24/7 guarantee.
