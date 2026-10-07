-- Installs that are not people.
--
-- One machine has been minting a fresh Docker Engine id about once a minute, each one
-- sending a single `start` and a single `events` beacon and never reporting again: ~1,600
-- "new installs" a day from one IP, three quarters of every weekly cohort, and a W1
-- retention of 10% that is 40% without it. No real IP comes close - the next busiest
-- peaked at 273 in a day, everything else under 30.
--
-- So an install is noise when 100 or more installs first opened the UI from the same IP
-- on the same day. Rate, not a lifetime count: a cloud NAT or VPN exit collects ids
-- slowly and forever, and would cross any fixed total eventually. The refresh sets the
-- flag and every derived count skips it; the beacons themselves are untouched.
--
-- first_ip is the IP of the install's first day, which the lifecycle's `metadata`
-- (always the latest beacon) cannot answer.
ALTER TABLE client_lifecycle ADD COLUMN IF NOT EXISTS first_ip text;
ALTER TABLE client_lifecycle ADD COLUMN IF NOT EXISTS noise boolean NOT NULL DEFAULT false;
CREATE INDEX IF NOT EXISTS idx_lifecycle_noise ON client_lifecycle (client_id) WHERE noise;
