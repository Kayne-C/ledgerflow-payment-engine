// Payment API under sustained load: POST /payments at a constant arrival rate between pre-funded accounts.
//   k6 run tests/load/payments.js -e BASE_URL=http://localhost:8090 -e RATE=500 -e DURATION=60s -e ACCOUNTS=200
// The API answers 202 after one atomic append (payment stream + outbox); the saga then runs asynchronously
// (see measure-e2e.sh for end-to-end throughput and saga latency).
import http from 'k6/http';
import { check } from 'k6';

const BASE = __ENV.BASE_URL || 'http://localhost:8090';
const KEY = __ENV.API_KEY || 'lf_demo_ops_4d8e1b6a0c';
const ACCOUNTS = Number(__ENV.ACCOUNTS || 200);
const headers = (extra = {}) => ({ headers: { 'X-Api-Key': KEY, 'Content-Type': 'application/json', ...extra } });

export const options = {
  setupTimeout: '5m',
  scenarios: {
    payments: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 500),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 200,
      maxVUs: 1000,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'http_req_duration{name:create-payment}': ['p(95)<100'],
  },
};

export function setup() {
  const accounts = [];
  for (let i = 0; i < ACCOUNTS; i++) {
    const account = http.post(`${BASE}/api/v1/accounts`, JSON.stringify({ holder: `Load ${i}`, currency: 'TRY' }), headers()).json('accountId');
    http.post(`${BASE}/api/v1/accounts/${account}/deposits`, JSON.stringify({ amount: 1000000, currency: 'TRY' }),
      headers({ 'Idempotency-Key': `load-fund-${account}` }));
    accounts.push(account);
  }
  return { accounts };
}

export default function (data) {
  const from = data.accounts[Math.floor(Math.random() * data.accounts.length)];
  let to = from;
  while (to === from) {
    to = data.accounts[Math.floor(Math.random() * data.accounts.length)];
  }

  const amount = (Math.floor(Math.random() * 10000) + 1) / 100; // 0.01 – 100.00 TRY
  const res = http.post(
    `${BASE}/api/v1/payments`,
    JSON.stringify({ fromAccountId: from, toAccountId: to, amount, currency: 'TRY', reference: `k6-${__VU}-${__ITER}` }),
    { ...headers({ 'Idempotency-Key': `k6-${__VU}-${__ITER}-${Date.now()}` }), tags: { name: 'create-payment' } },
  );

  check(res, { 'accepted (202)': (r) => r.status === 202 });
}
