import { check, sleep } from "k6";
import http from "k6/http";

export const options = {
  scenarios: {
    tsurugi_insert_test: {
      executor: "constant-vus",
      vus: 4,
      duration: "30s",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<30000"],
  },
};

const BASE_URL = "http://127.0.0.1:8931";

export default function () {
  const res = http.post(`${BASE_URL}/tsurugi/insert`, null, {
    headers: {
      "Content-Type": "application/json",
    },
    timeout: "60s",
  });

  check(res, {
    "status is 200": (r) => r.status === 200,
  });

  sleep(1);
}
