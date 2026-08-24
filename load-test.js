import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '10s', target: 500 },  // Ramp-up
    { duration: '30s', target: 2000 }, // Sustained load
    { duration: '10s', target: 0 },    // Ramp-down
  ],
  thresholds: {
    http_req_duration: ['p(95)<10'], // 95% of requests must complete below 10ms
  },
};

export default function () {
  const url = 'http://localhost:8080/ingest';
  const payload = JSON.stringify({
    id: `req_${__VU}_${__ITER}`,
    source: 'ai_factory_sensor',
    ev_value: Math.random() > 0.1 ? 2.5 : -7.0, // Tests both valid and dropped packets
    payload: 'tensor_stream_chunk_001',
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(url, payload, params);
  check(res, {
    'status is 202 or 204': (r) => r.status === 202 || r.status === 204,
  });

  sleep(0.005);
}

