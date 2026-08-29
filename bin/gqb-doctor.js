#!/usr/bin/env node
import { runDoctorCli } from "../src/gqb/doctor.js";

runDoctorCli().catch((error) => {
  process.stderr.write(`${JSON.stringify({
    event: "gqb.doctor.failed",
    ts: new Date().toISOString(),
    component: "gqb.doctor",
    trace_id: null,
    level: "error",
    diagnosis: "DOCTOR_FAILED",
    details: { message: error.message }
  })}\n`);
  process.exitCode = 1;
});
